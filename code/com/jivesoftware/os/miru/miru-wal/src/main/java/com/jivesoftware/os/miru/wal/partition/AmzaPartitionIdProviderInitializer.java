package com.jivesoftware.os.miru.wal.partition;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Optional;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.service.AmzaServiceInitializer;
import com.jivesoftware.os.amza.service.AmzaServiceInitializer.AmzaServiceConfig;
import com.jivesoftware.os.amza.service.discovery.AmzaDiscovery;
import com.jivesoftware.os.amza.service.replication.MemoryBackedHighWaterMarks;
import com.jivesoftware.os.amza.service.replication.SendFailureListener;
import com.jivesoftware.os.amza.service.replication.TakeFailureListener;
import com.jivesoftware.os.amza.service.stats.AmzaStats;
import com.jivesoftware.os.amza.shared.AmzaInstance;
import com.jivesoftware.os.amza.shared.MemoryWALIndex;
import com.jivesoftware.os.amza.shared.NoOpWALIndex;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.RowChanges;
import com.jivesoftware.os.amza.shared.RowsChanged;
import com.jivesoftware.os.amza.shared.UpdatesSender;
import com.jivesoftware.os.amza.shared.UpdatesTaker;
import com.jivesoftware.os.amza.shared.WALIndex;
import com.jivesoftware.os.amza.shared.WALIndexProvider;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALReplicator;
import com.jivesoftware.os.amza.shared.WALStorage;
import com.jivesoftware.os.amza.shared.WALStorageProvider;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.amza.storage.FstMarshaller;
import com.jivesoftware.os.amza.storage.IndexedWAL;
import com.jivesoftware.os.amza.storage.NonIndexWAL;
import com.jivesoftware.os.amza.storage.binary.BinaryRowIOProvider;
import com.jivesoftware.os.amza.storage.binary.BinaryRowMarshaller;
import com.jivesoftware.os.amza.storage.binary.BinaryRowsTx;
import com.jivesoftware.os.amza.storage.binary.RowIOProvider;
import com.jivesoftware.os.amza.transport.http.replication.HttpUpdatesSender;
import com.jivesoftware.os.amza.transport.http.replication.HttpUpdatesTaker;
import com.jivesoftware.os.amza.transport.http.replication.endpoints.AmzaReplicationRestEndpoints;
import com.jivesoftware.os.amza.transport.tcp.replication.serialization.MessagePayload;
import com.jivesoftware.os.amza.transport.tcp.replication.serialization.MessagePayloadSerializer;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.upena.main.Deployable;
import de.ruedigermoeller.serialization.FSTConfiguration;
import java.io.File;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.merlin.config.Config;
import org.merlin.config.defaults.IntDefault;
import org.merlin.config.defaults.LongDefault;
import org.merlin.config.defaults.StringDefault;

/**
 * @author jonathan.colt
 */
public class AmzaPartitionIdProviderInitializer {

    public static interface AmzaPartitionIdProviderConfig extends Config {

        @StringDefault("./var/data/")
        public String getWorkingDirectory();

        public void setWorkingDirectory(String dir);

        @IntDefault(1)
        public int getReplicationFactor();

        public void setReplicationFactor(int factor);

        @IntDefault(1)
        public int getTakeFromFactor();

        public void setTakeFromFactor(int factor);

        @IntDefault(1000)
        public int getResendReplicasIntervalInMillis();

        @IntDefault(1000)
        public int getApplyReplicasIntervalInMillis();

        @IntDefault(1000)
        public int getTakeFromNeighborsIntervalInMillis();

        @LongDefault(60_000)
        public long getCheckIfCompactionIsNeededIntervalInMillis();

        @LongDefault(1 * 24 * 60 * 60 * 1000L)
        public long getCompactTombstoneIfOlderThanNMillis();

    }

    public AmzaService initialize(Deployable deployable,
        int instanceId,
        String hostName,
        int port,
        String clusterName,
        AmzaPartitionIdProviderConfig config) throws Exception {

        String multicastGroup = System.getProperty("amza.discovery.group", "225.4.5.7");
        int multicastPort = Integer.parseInt(System.getProperty("amza.discovery.port", "1224"));

        RingHost ringHost = new RingHost(hostName, port);

        final TimestampedOrderIdProvider orderIdProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(instanceId));

        final ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(SerializationFeature.INDENT_OUTPUT, false);

        final WALIndexProvider walIndexProvider = new WALIndexProvider() {
            @Override
            public WALIndex createIndex(RegionName regionName) throws Exception {
                NavigableMap<WALKey, WALValue> navigableMap = new ConcurrentSkipListMap<>();
                return new MemoryWALIndex(navigableMap);
            }
        };

        final BinaryRowMarshaller rowMarshaller = new BinaryRowMarshaller();
        final BinaryRowIOProvider rowIoProvider = new BinaryRowIOProvider();
        WALStorageProvider walStorageProvider = new WALStorageProvider() {
            @Override
            public WALStorage create(File workingDirectory, String tableDomain, RegionName regionName, WALReplicator walReplicator) throws Exception {
                final File directory = new File(workingDirectory, tableDomain);
                directory.mkdirs();
                File file = new File(directory, regionName.getRegionName() + ".kvt");

                return new IndexedWAL(regionName,
                    orderIdProvider,
                    rowMarshaller,
                    new BinaryRowsTx(file,
                        regionName.getRegionName() + ".kvt",
                        rowIoProvider,
                        rowMarshaller,
                        walIndexProvider,
                        100), //TODO configure back-repair (per table?)
                    walReplicator,
                    1_000); //TODO configure max updates
            }
        };

        final WALIndexProvider tmpWALIndexProvider = new WALIndexProvider() {

            @Override
            public WALIndex createIndex(RegionName regionName) throws Exception {
                return new NoOpWALIndex();
            }
        };

        WALStorageProvider tmpWALStorageProvider = new WALStorageProvider() {
            @Override
            public WALStorage create(File workingDirectory,
                String domain,
                RegionName regionName,
                WALReplicator rowReplicator) throws Exception {

                final File directory = new File(workingDirectory, domain);
                directory.mkdirs();
                RowIOProvider rowIOProvider = new BinaryRowIOProvider();
                return new NonIndexWAL(regionName,
                    orderIdProvider,
                    rowMarshaller,
                    new BinaryRowsTx(directory,
                        regionName.getRegionName() + ".kvt",
                        rowIOProvider,
                        rowMarshaller,
                        tmpWALIndexProvider,
                        100)); //TODO configure back-repair (per table?)
            }
        };

        FstMarshaller marshaller = new FstMarshaller(FSTConfiguration.getDefaultConfiguration());
        marshaller.registerSerializer(MessagePayload.class, new MessagePayloadSerializer());

        UpdatesSender changeSetSender = new HttpUpdatesSender();
        UpdatesTaker tableTaker = new HttpUpdatesTaker();

        final AmzaServiceConfig amzaServiceConfig = new AmzaServiceConfig();
        amzaServiceConfig.workingDirectory = config.getWorkingDirectory();
        amzaServiceConfig.replicationFactor = config.getReplicationFactor();
        amzaServiceConfig.takeFromFactor = config.getTakeFromFactor();
        amzaServiceConfig.resendReplicasIntervalInMillis = config.getResendReplicasIntervalInMillis();
        amzaServiceConfig.applyReplicasIntervalInMillis = config.getApplyReplicasIntervalInMillis();
        amzaServiceConfig.takeFromNeighborsIntervalInMillis = config.getTakeFromNeighborsIntervalInMillis();
        amzaServiceConfig.checkIfCompactionIsNeededIntervalInMillis = config.getCheckIfCompactionIsNeededIntervalInMillis();
        amzaServiceConfig.compactTombstoneIfOlderThanNMillis = config.getCompactTombstoneIfOlderThanNMillis();

        AmzaService amzaService = new AmzaServiceInitializer().initialize(amzaServiceConfig,
            new AmzaStats(),
            ringHost,
            orderIdProvider,
            new com.jivesoftware.os.amza.storage.FstMarshaller(FSTConfiguration.getDefaultConfiguration()),
            walStorageProvider,
            tmpWALStorageProvider,
            tmpWALStorageProvider,
            changeSetSender,
            tableTaker,
            new MemoryBackedHighWaterMarks(),
            Optional.<SendFailureListener>absent(),
            Optional.<TakeFailureListener>absent(),
            new RowChanges() {
                @Override
                public void changes(RowsChanged changes) throws Exception {
                }
            });

        amzaService.start();

        System.out.println("-----------------------------------------------------------------------");
        System.out.println("|      Amza Service Online");
        System.out.println("-----------------------------------------------------------------------");

        deployable.addEndpoints(AmzaReplicationRestEndpoints.class);
        deployable.addInjectables(AmzaInstance.class, amzaService);

        if (clusterName != null) {
            AmzaDiscovery amzaDiscovery = new AmzaDiscovery(amzaService.getAmzaRing(), ringHost, clusterName, multicastGroup, multicastPort);
            amzaDiscovery.start();
            System.out.println("-----------------------------------------------------------------------");
            System.out.println("|      Amza Service Discovery Online");
            System.out.println("-----------------------------------------------------------------------");
        } else {
            System.out.println("-----------------------------------------------------------------------");
            System.out.println("|     Amze Service is in manual Discovery mode.  No cluster name was specified");
            System.out.println("-----------------------------------------------------------------------");
        }
        return amzaService;
    }
}
