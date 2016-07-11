package com.jivesoftware.os.miru.amza;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.jivesoftware.os.amza.api.BAInterner;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.ring.RingHost;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.scan.RowChanges;
import com.jivesoftware.os.amza.berkeleydb.BerkeleyDBWALIndexProvider;
import com.jivesoftware.os.amza.client.http.AmzaClientProvider;
import com.jivesoftware.os.amza.client.http.HttpPartitionClientFactory;
import com.jivesoftware.os.amza.client.http.HttpPartitionHostsProvider;
import com.jivesoftware.os.amza.client.http.RingHostHttpClientProvider;
import com.jivesoftware.os.amza.lab.pointers.LABPointerIndexConfig;
import com.jivesoftware.os.amza.lab.pointers.LABPointerIndexWALIndexProvider;
import com.jivesoftware.os.amza.service.AmzaInstance;
import com.jivesoftware.os.amza.service.AmzaRingStoreWriter;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.service.AmzaServiceInitializer.AmzaServiceConfig;
import com.jivesoftware.os.amza.service.EmbeddedAmzaServiceInitializer;
import com.jivesoftware.os.amza.service.SickPartitions;
import com.jivesoftware.os.amza.service.replication.TakeFailureListener;
import com.jivesoftware.os.amza.service.replication.http.HttpAvailableRowsTaker;
import com.jivesoftware.os.amza.service.replication.http.HttpRowsTaker;
import com.jivesoftware.os.amza.service.replication.http.endpoints.AmzaReplicationRestEndpoints;
import com.jivesoftware.os.amza.service.ring.AmzaRingReader;
import com.jivesoftware.os.amza.service.ring.AmzaRingWriter;
import com.jivesoftware.os.amza.service.stats.AmzaStats;
import com.jivesoftware.os.amza.service.storage.PartitionPropertyMarshaller;
import com.jivesoftware.os.amza.service.take.AvailableRowsTaker;
import com.jivesoftware.os.amza.service.take.RowsTakerFactory;
import com.jivesoftware.os.amza.ui.AmzaUIInitializer;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.deployable.Deployable;
import com.jivesoftware.os.routing.bird.health.HealthCheckResponse;
import com.jivesoftware.os.routing.bird.health.HealthCheckResponseImpl;
import com.jivesoftware.os.routing.bird.health.checkers.SickThreads;
import com.jivesoftware.os.routing.bird.http.client.HttpClient;
import com.jivesoftware.os.routing.bird.http.client.HttpClientException;
import com.jivesoftware.os.routing.bird.http.client.HttpDeliveryClientHealthProvider;
import com.jivesoftware.os.routing.bird.http.client.HttpRequestHelperUtils;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.routing.bird.http.client.TenantRoutingHttpClientInitializer;
import com.jivesoftware.os.routing.bird.server.util.Resource;
import com.jivesoftware.os.routing.bird.shared.ConnectionDescriptor;
import com.jivesoftware.os.routing.bird.shared.ConnectionDescriptors;
import com.jivesoftware.os.routing.bird.shared.HostPort;
import com.jivesoftware.os.routing.bird.shared.InstanceDescriptor;
import com.jivesoftware.os.routing.bird.shared.TenantRoutingProvider;
import com.jivesoftware.os.routing.bird.shared.TenantsServiceConnectionDescriptorProvider;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.merlin.config.defaults.BooleanDefault;
import org.merlin.config.defaults.LongDefault;

/**
 *
 */
public class MiruAmzaServiceInitializer {

    public interface MiruAmzaLabConfig extends LABPointerIndexConfig {

        @Override
        @BooleanDefault(true)
        boolean getUseMemMap();

        @Override
        @LongDefault(10_485_760L)
        long getSplitWhenValuesAndKeysTotalExceedsNBytes();
    }

    public AmzaService initialize(Deployable deployable,
        String routesHost,
        int routesPort,
        String connectionsHealthEndpoint,
        int instanceId,
        String instanceKey,
        String serviceName,
        String datacenterName,
        String rackName,
        String hostName,
        int port,
        String clusterName,
        MiruAmzaServiceConfig config,
        boolean useAmzaDiscovery,
        RowChanges allRowChanges) throws Exception {

        RingMember ringMember = new RingMember(
            Strings.padStart(String.valueOf(instanceId), 5, '0') + "_" + instanceKey);
        RingHost ringHost = new RingHost(datacenterName, rackName, hostName, port);

        SnowflakeIdPacker idPacker = new SnowflakeIdPacker();
        JiveEpochTimestampProvider timestampProvider = new JiveEpochTimestampProvider();
        TimestampedOrderIdProvider orderIdProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(instanceId),
            idPacker, timestampProvider);

        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(SerializationFeature.INDENT_OUTPUT, false);

        AmzaStats amzaStats = new AmzaStats();
        BAInterner baInterner = new BAInterner();
        RowsTakerFactory rowsTakerFactory = () -> new HttpRowsTaker(amzaStats, baInterner, (int) config.getInterruptBlockingReadsIfLingersForNMillis());
        AvailableRowsTaker availableRowsTaker = new HttpAvailableRowsTaker(baInterner, (int) config.getInterruptBlockingReadsIfLingersForNMillis());

        AmzaServiceConfig amzaServiceConfig = new AmzaServiceConfig();
        amzaServiceConfig.workingDirectories = config.getWorkingDirectories().split(",");
        amzaServiceConfig.checkIfCompactionIsNeededIntervalInMillis = config.getCheckIfCompactionIsNeededIntervalInMillis();
        amzaServiceConfig.deltaStripeCompactionIntervalInMillis = config.getDeltaStripeCompactionIntervalInMillis();
        amzaServiceConfig.corruptionParanoiaFactor = config.getCorruptionParanoiaFactor();
        amzaServiceConfig.maxUpdatesBeforeDeltaStripeCompaction = config.getMaxUpdatesBeforeDeltaStripeCompaction();
        amzaServiceConfig.numberOfTakerThreads = config.getNumberOfTakerThreads();
        amzaServiceConfig.hardFsync = config.getHardFsync();
        amzaServiceConfig.takeSlowThresholdInMillis = config.getTakeSlowThresholdInMillis();
        amzaServiceConfig.rackDistributionEnabled = config.getRackDistributionEnabled();

        MiruAmzaLabConfig amzaLabConfig = deployable.config(MiruAmzaLabConfig.class);

        PartitionPropertyMarshaller partitionPropertyMarshaller = new PartitionPropertyMarshaller() {

            @Override
            public PartitionProperties fromBytes(byte[] bytes) {
                try {
                    return mapper.readValue(bytes, PartitionProperties.class);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public byte[] toBytes(PartitionProperties partitionProperties) {
                try {
                    return mapper.writeValueAsBytes(partitionProperties);
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            }
        };

        SickThreads sickThreads = new SickThreads();
        SickPartitions sickPartitions = new SickPartitions();

        String blacklist = config.getBlacklistRingMembers();
        Set<RingMember> blacklistRingMembers = Sets.newHashSet();
        for (String b : blacklist != null ? blacklist.split("\\s*,\\s*") : new String[0]) {
            if (b != null) {
                b = b.trim();
                if (!b.isEmpty()) {
                    blacklistRingMembers.add(new RingMember(b));
                }
            }
        }

        AmzaService amzaService = new EmbeddedAmzaServiceInitializer().initialize(amzaServiceConfig,
            baInterner,
            amzaStats,
            sickThreads,
            sickPartitions,
            ringMember,
            ringHost,
            blacklistRingMembers,
            orderIdProvider,
            idPacker,
            partitionPropertyMarshaller,
            (workingIndexDirectories,
                indexProviderRegistry,
                ephemeralRowIOProvider,
                persistentRowIOProvider,
                numberOfStripes) -> {
                indexProviderRegistry.register(new LABPointerIndexWALIndexProvider(amzaLabConfig, "lab", numberOfStripes, workingIndexDirectories),
                    persistentRowIOProvider);
                indexProviderRegistry.register(new BerkeleyDBWALIndexProvider("berkeleydb", numberOfStripes, workingIndexDirectories),
                    persistentRowIOProvider);
            },
            availableRowsTaker,
            rowsTakerFactory,
            Optional.<TakeFailureListener>absent(),
            allRowChanges);

        RoutingBirdAmzaDiscovery routingBirdAmzaDiscovery = new RoutingBirdAmzaDiscovery(deployable,
            serviceName,
            amzaService,
            config.getDiscoveryIntervalMillis(),
            blacklistRingMembers);

        amzaService.start(ringMember, ringHost);

        System.out.println("-----------------------------------------------------------------------");
        System.out.println("|      Amza Service Online");
        System.out.println("-----------------------------------------------------------------------");

        if (useAmzaDiscovery) {
            routingBirdAmzaDiscovery.start();
        }

        System.out.println("-----------------------------------------------------------------------");
        System.out.println("|     Amza Service is in Routing Bird Discovery mode");
        System.out.println("-----------------------------------------------------------------------");

        HttpDeliveryClientHealthProvider clientHealthProvider = new HttpDeliveryClientHealthProvider(instanceKey,
            HttpRequestHelperUtils.buildRequestHelper(routesHost, routesPort),
            connectionsHealthEndpoint, 5_000, 100);

        TenantRoutingHttpClientInitializer<String> tenantRoutingHttpClientInitializer = new TenantRoutingHttpClientInitializer<>();
        TenantAwareHttpClient<String> httpClient = tenantRoutingHttpClientInitializer.initialize(
            deployable.getTenantRoutingProvider().getConnections(serviceName, "main", 10_000), // TODO config
            clientHealthProvider,
            10,
            10_000); // TODO expose to conf

        AmzaClientProvider<HttpClient, HttpClientException> clientProvider = new AmzaClientProvider<>(
            new HttpPartitionClientFactory(baInterner),
            new HttpPartitionHostsProvider(baInterner, httpClient, mapper),
            new RingHostHttpClientProvider(httpClient),
            Executors.newCachedThreadPool(),
            10_000, //TODO expose to conf
            -1,
            -1);

        new AmzaUIInitializer().initialize(clusterName, ringHost, amzaService, clientProvider, amzaStats, timestampProvider, idPacker,
            new AmzaUIInitializer.InjectionCallback() {

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
        deployable.addInjectables(AmzaRingWriter.class, amzaService.getRingWriter());
        deployable.addInjectables(AmzaRingReader.class, amzaService.getRingReader());
        deployable.addInjectables(AmzaInstance.class, amzaService);
        deployable.addInjectables(BAInterner.class, baInterner);

        Resource staticResource = new Resource(null)
            .addClasspathResource("resources/static/amza")
            .setContext("/static/amza");
        deployable.addResource(staticResource);

        deployable.addHealthCheck(() -> {
            Map<Thread, Throwable> sickThread = sickThreads.getSickThread();
            if (sickThread.isEmpty()) {
                return new HealthCheckResponseImpl("sick>threads", 1.0, "Healthy", "No sick threads", "", System.currentTimeMillis());
            } else {
                return new HealthCheckResponse() {

                    @Override
                    public String getName() {
                        return "sick>thread";
                    }

                    @Override
                    public double getHealth() {
                        return 0;
                    }

                    @Override
                    public String getStatus() {
                        return "There are " + sickThread.size() + " sick threads.";
                    }

                    @Override
                    public String getDescription() {
                        StringBuilder sb = new StringBuilder();
                        for (Map.Entry<Thread, Throwable> entry : sickThread.entrySet()) {
                            sb.append("thread:").append(entry.getKey()).append(" cause:").append(entry.getValue());
                        }
                        return sb.toString();
                    }

                    @Override
                    public String getResolution() {
                        return "Look at the logs and see if you can resolve the issue.";
                    }

                    @Override
                    public long getTimestamp() {
                        return System.currentTimeMillis();
                    }
                };
            }
        });

        deployable.addHealthCheck(() -> {
            Map<VersionedPartitionName, Throwable> sickPartition = sickPartitions.getSickPartitions();
            if (sickPartition.isEmpty()) {
                return new HealthCheckResponseImpl("sick>partitions", 1.0, "Healthy", "No sick partitions", "", System.currentTimeMillis());
            } else {
                return new HealthCheckResponse() {

                    @Override
                    public String getName() {
                        return "sick>partition";
                    }

                    @Override
                    public double getHealth() {
                        return 0;
                    }

                    @Override
                    public String getStatus() {
                        return "There are " + sickPartition.size() + " sick partitions.";
                    }

                    @Override
                    public String getDescription() {
                        StringBuilder sb = new StringBuilder();
                        for (Map.Entry<VersionedPartitionName, Throwable> entry : sickPartition.entrySet()) {
                            sb.append("partition:").append(entry.getKey()).append(" cause:").append(entry.getValue());
                        }
                        return sb.toString();
                    }

                    @Override
                    public String getResolution() {
                        return "Look at the logs and see if you can resolve the issue.";
                    }

                    @Override
                    public long getTimestamp() {
                        return System.currentTimeMillis();
                    }
                };
            }
        });

        return amzaService;
    }

    static class RoutingBirdAmzaDiscovery implements Runnable {

        private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
        private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);

        private final Deployable deployable;
        private final String serviceName;
        private final AmzaService amzaService;
        private final long discoveryIntervalMillis;
        private final Set<RingMember> blacklistRingMembers;

        public RoutingBirdAmzaDiscovery(Deployable deployable,
            String serviceName,
            AmzaService amzaService,
            long discoveryIntervalMillis,
            Set<RingMember> blacklistRingMembers) {
            this.deployable = deployable;
            this.serviceName = serviceName;
            this.amzaService = amzaService;
            this.discoveryIntervalMillis = discoveryIntervalMillis;
            this.blacklistRingMembers = blacklistRingMembers;
        }

        public void start() {
            scheduledExecutorService.scheduleWithFixedDelay(this, 0, discoveryIntervalMillis, TimeUnit.MILLISECONDS);
        }

        public void stop() {
            scheduledExecutorService.shutdownNow();
        }

        @Override
        public void run() {
            try {
                TenantRoutingProvider tenantRoutingProvider = deployable.getTenantRoutingProvider();
                TenantsServiceConnectionDescriptorProvider connections = tenantRoutingProvider.getConnections(serviceName, "main", 10_000); // TODO config
                ConnectionDescriptors selfConnections = connections.getConnections("");
                for (ConnectionDescriptor connectionDescriptor : selfConnections.getConnectionDescriptors()) {

                    InstanceDescriptor routingInstanceDescriptor = connectionDescriptor.getInstanceDescriptor();
                    RingMember routingRingMember = new RingMember(
                        Strings.padStart(String.valueOf(routingInstanceDescriptor.instanceName), 5, '0') + "_" + routingInstanceDescriptor.instanceKey);

                    if (!blacklistRingMembers.contains(routingRingMember)) {
                        HostPort hostPort = connectionDescriptor.getHostPort();
                        AmzaRingStoreWriter ringWriter = amzaService.getRingWriter();
                        ringWriter.register(routingRingMember, new RingHost(routingInstanceDescriptor.datacenter, routingInstanceDescriptor.rack,
                            hostPort.getHost(), hostPort.getPort()), -1);
                        ringWriter.addRingMember(AmzaRingReader.SYSTEM_RING, routingRingMember);
                    }
                }
            } catch (Exception x) {
                LOG.warn("Failed while calling routing bird discovery.", x);
            }
        }
    }
}
