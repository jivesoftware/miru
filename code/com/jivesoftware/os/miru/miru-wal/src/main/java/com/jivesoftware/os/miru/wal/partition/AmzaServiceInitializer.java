package com.jivesoftware.os.miru.wal.partition;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Optional;
import com.jivesoftware.os.amza.mapdb.MapdbWALIndexProvider;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.service.EmbeddedAmzaServiceInitializer;
import com.jivesoftware.os.amza.service.discovery.AmzaDiscovery;
import com.jivesoftware.os.amza.service.replication.SendFailureListener;
import com.jivesoftware.os.amza.service.replication.TakeFailureListener;
import com.jivesoftware.os.amza.service.stats.AmzaStats;
import com.jivesoftware.os.amza.shared.AmzaInstance;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.RowChanges;
import com.jivesoftware.os.amza.shared.UpdatesSender;
import com.jivesoftware.os.amza.shared.UpdatesTaker;
import com.jivesoftware.os.amza.shared.WALIndexProvider;
import com.jivesoftware.os.amza.transport.http.replication.HttpUpdatesSender;
import com.jivesoftware.os.amza.transport.http.replication.HttpUpdatesTaker;
import com.jivesoftware.os.amza.transport.http.replication.endpoints.AmzaReplicationRestEndpoints;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.upena.main.Deployable;
import org.merlin.config.Config;
import org.merlin.config.defaults.IntDefault;
import org.merlin.config.defaults.LongDefault;
import org.merlin.config.defaults.StringDefault;

/**
 * @author jonathan.colt
 */
public class AmzaServiceInitializer {

    public static interface AmzaServiceConfig extends Config {

        @StringDefault("./var/amza/wal/data/")
        public String getWorkingDirectories();

        public void setWorkingDirectories(String dir);

        @StringDefault("./var/amza/wal/index/")
        public String getIndexDirectories();

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
        AmzaServiceConfig config,
        RowChanges allRowChanges) throws Exception {

        String multicastGroup = System.getProperty("amza.discovery.group", "225.4.5.7");
        int multicastPort = Integer.parseInt(System.getProperty("amza.discovery.port", "1225")); //TODO expose to config

        RingHost ringHost = new RingHost(hostName, port);

        final TimestampedOrderIdProvider orderIdProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(instanceId));

        final ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(SerializationFeature.INDENT_OUTPUT, false);

        final WALIndexProvider walIndexProvider = new MapdbWALIndexProvider(config.getIndexDirectories().split(","));

        UpdatesSender changeSetSender = new HttpUpdatesSender();
        UpdatesTaker tableTaker = new HttpUpdatesTaker();

        final com.jivesoftware.os.amza.service.AmzaServiceInitializer.AmzaServiceConfig amzaServiceConfig =
            new com.jivesoftware.os.amza.service.AmzaServiceInitializer.AmzaServiceConfig();
        amzaServiceConfig.workingDirectories = config.getWorkingDirectories().split(",");
        amzaServiceConfig.replicationFactor = config.getReplicationFactor();
        amzaServiceConfig.takeFromFactor = config.getTakeFromFactor();
        amzaServiceConfig.resendReplicasIntervalInMillis = config.getResendReplicasIntervalInMillis();
        amzaServiceConfig.applyReplicasIntervalInMillis = config.getApplyReplicasIntervalInMillis();
        amzaServiceConfig.takeFromNeighborsIntervalInMillis = config.getTakeFromNeighborsIntervalInMillis();
        amzaServiceConfig.checkIfCompactionIsNeededIntervalInMillis = config.getCheckIfCompactionIsNeededIntervalInMillis();
        amzaServiceConfig.compactTombstoneIfOlderThanNMillis = config.getCompactTombstoneIfOlderThanNMillis();

        AmzaService amzaService = new EmbeddedAmzaServiceInitializer().initialize(amzaServiceConfig,
            new AmzaStats(),
            ringHost,
            orderIdProvider,
            walIndexProvider,
            changeSetSender,
            tableTaker,
            Optional.<SendFailureListener>absent(),
            Optional.<TakeFailureListener>absent(),
            allRowChanges);

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
