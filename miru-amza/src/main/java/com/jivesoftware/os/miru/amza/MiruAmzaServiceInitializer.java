package com.jivesoftware.os.miru.amza;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.jivesoftware.os.amza.berkeleydb.BerkeleyDBWALIndexProvider;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.service.EmbeddedAmzaServiceInitializer;
import com.jivesoftware.os.amza.service.WALIndexProviderRegistry;
import com.jivesoftware.os.amza.service.discovery.AmzaDiscovery;
import com.jivesoftware.os.amza.service.replication.SendFailureListener;
import com.jivesoftware.os.amza.service.replication.TakeFailureListener;
import com.jivesoftware.os.amza.service.storage.PartitionPropertyMarshaller;
import com.jivesoftware.os.amza.shared.AmzaInstance;
import com.jivesoftware.os.amza.shared.partition.PartitionProperties;
import com.jivesoftware.os.amza.shared.ring.AmzaRing;
import com.jivesoftware.os.amza.shared.ring.RingHost;
import com.jivesoftware.os.amza.shared.ring.RingMember;
import com.jivesoftware.os.amza.shared.scan.RowChanges;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import com.jivesoftware.os.amza.shared.take.HighwaterStorage;
import com.jivesoftware.os.amza.shared.take.UpdatesTaker;
import com.jivesoftware.os.amza.transport.http.replication.HttpUpdatesTaker;
import com.jivesoftware.os.amza.transport.http.replication.endpoints.AmzaReplicationRestEndpoints;
import com.jivesoftware.os.amza.ui.AmzaUIInitializer;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.routing.bird.deployable.Deployable;
import com.jivesoftware.os.routing.bird.server.util.Resource;

/**
 *
 */
public class MiruAmzaServiceInitializer {

    public AmzaService initialize(final Deployable deployable,
        int instanceId,
        String instanceKey,
        String hostName,
        int port,
        String clusterName,
        MiruAmzaServiceConfig config,
        RowChanges allRowChanges) throws Exception {

        String multicastGroup = System.getProperty("amza.discovery.group", config.getAmzaDiscoveryGroup());
        int multicastPort = Integer.parseInt(System.getProperty("amza.discovery.port", String.valueOf(config.getAmzaDiscoveryPort())));

        RingMember ringMember = new RingMember(
            Strings.padStart(String.valueOf(instanceId), 5, '0') + "_" + instanceKey);
        RingHost ringHost = new RingHost(hostName, port);

        final TimestampedOrderIdProvider orderIdProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(instanceId));

        final ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(SerializationFeature.INDENT_OUTPUT, false);

        WALIndexProviderRegistry indexProviderRegistry = new WALIndexProviderRegistry();
        String[] walIndexDirs = config.getIndexDirectories().split(",");
        indexProviderRegistry.register("berkeleydb", new BerkeleyDBWALIndexProvider(walIndexDirs, walIndexDirs.length));

        AmzaStats amzaStats = new AmzaStats();
        UpdatesTaker tableTaker = new HttpUpdatesTaker(amzaStats);

        final com.jivesoftware.os.amza.service.AmzaServiceInitializer.AmzaServiceConfig amzaServiceConfig =
            new com.jivesoftware.os.amza.service.AmzaServiceInitializer.AmzaServiceConfig();
        amzaServiceConfig.workingDirectories = config.getWorkingDirectories().split(",");
        amzaServiceConfig.checkIfCompactionIsNeededIntervalInMillis = config.getCheckIfCompactionIsNeededIntervalInMillis();
        amzaServiceConfig.deltaStripeCompactionIntervalInMillis = config.getDeltaStripeCompactionIntervalInMillis();
        amzaServiceConfig.takeFromNeighborsIntervalInMillis = config.getTakeFromNeighborsIntervalInMillis();
        amzaServiceConfig.compactTombstoneIfOlderThanNMillis = config.getCompactTombstoneIfOlderThanNMillis();
        amzaServiceConfig.corruptionParanoiaFactor = config.getCorruptionParanoiaFactor();
        amzaServiceConfig.maxUpdatesBeforeDeltaStripeCompaction = config.getMaxUpdatesBeforeDeltaStripeCompaction();
        amzaServiceConfig.numberOfDeltaStripes = amzaServiceConfig.workingDirectories.length;
        amzaServiceConfig.numberOfCompactorThreads = config.getNumberOfCompactorThreads();
        amzaServiceConfig.numberOfTakerThreads = config.getNumberOfTakerThreads();
        amzaServiceConfig.numberOfAckerThreads = config.getNumberOfAckerThreads();
        amzaServiceConfig.hardFsync = config.getHardFsync();

        PartitionPropertyMarshaller partitionPropertyMarshaller = new PartitionPropertyMarshaller() {

            @Override
            public PartitionProperties fromBytes(byte[] bytes) throws Exception {
                return mapper.readValue(bytes, PartitionProperties.class);
            }

            @Override
            public byte[] toBytes(PartitionProperties partitionProperties) throws Exception {
                return mapper.writeValueAsBytes(partitionProperties);
            }
        };

        AmzaService amzaService = new EmbeddedAmzaServiceInitializer().initialize(amzaServiceConfig,
            amzaStats,
            ringMember,
            ringHost,
            orderIdProvider,
            partitionPropertyMarshaller,
            indexProviderRegistry,
            tableTaker,
            Optional.<SendFailureListener>absent(),
            Optional.<TakeFailureListener>absent(),
            allRowChanges);

        amzaService.start();

        System.out.println("-----------------------------------------------------------------------");
        System.out.println("|      Amza Service Online");
        System.out.println("-----------------------------------------------------------------------");

        new AmzaUIInitializer().initialize(clusterName, ringHost, amzaService, amzaStats, new AmzaUIInitializer.InjectionCallback() {

            @Override
            public void addEndpoint(Class clazz) {
                System.out.println("Adding endpoint=" + clazz);
                deployable.addEndpoints(clazz);
            }

            @Override
            public void addInjectable(Class clazz, Object instance) {
                System.out.println("Injecting " + clazz + " " + instance);
                deployable.addInjectables(clazz, instance);
            }
        });

        deployable.addEndpoints(AmzaReplicationRestEndpoints.class);
        deployable.addInjectables(AmzaRing.class, amzaService.getAmzaHostRing());
        deployable.addInjectables(AmzaInstance.class, amzaService);
        deployable.addInjectables(HighwaterStorage.class, amzaService.getHighwaterMarks());

        Resource staticResource = new Resource(null)
            .addClasspathResource("resources/static/amza")
            .setContext("/static/amza");
        deployable.addResource(staticResource);

        if (clusterName != null && multicastPort > 0) {
            AmzaDiscovery amzaDiscovery = new AmzaDiscovery(amzaService.getAmzaHostRing(), clusterName, multicastGroup, multicastPort);
            amzaDiscovery.start();
            System.out.println("-----------------------------------------------------------------------");
            System.out.println("|      Amza Service Discovery Online");
            System.out.println("-----------------------------------------------------------------------");
        } else {
            System.out.println("-----------------------------------------------------------------------");
            System.out.println("|     Amza Service is in manual Discovery mode.  No cluster name was specified or discovery port not set");
            System.out.println("-----------------------------------------------------------------------");
        }
        return amzaService;
    }
}
