package com.jivesoftware.os.miru.amza;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.ring.RingHost;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.scan.RowChanges;
import com.jivesoftware.os.amza.berkeleydb.BerkeleyDBWALIndexProvider;
import com.jivesoftware.os.amza.client.http.AmzaClientProvider;
import com.jivesoftware.os.amza.client.http.HttpPartitionClientFactory;
import com.jivesoftware.os.amza.client.http.HttpPartitionHostsProvider;
import com.jivesoftware.os.amza.client.http.RingHostHttpClientProvider;
import com.jivesoftware.os.amza.service.AmzaInstance;
import com.jivesoftware.os.amza.service.AmzaRingStoreWriter;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.service.AmzaServiceInitializer.AmzaServiceConfig;
import com.jivesoftware.os.amza.service.EmbeddedAmzaServiceInitializer;
import com.jivesoftware.os.amza.service.SickThreads;
import com.jivesoftware.os.amza.service.WALIndexProviderRegistry;
import com.jivesoftware.os.amza.service.discovery.AmzaDiscovery;
import com.jivesoftware.os.amza.service.replication.TakeFailureListener;
import com.jivesoftware.os.amza.service.replication.http.HttpAvailableRowsTaker;
import com.jivesoftware.os.amza.service.replication.http.HttpRowsTaker;
import com.jivesoftware.os.amza.service.replication.http.endpoints.AmzaReplicationRestEndpoints;
import com.jivesoftware.os.amza.service.ring.AmzaRingReader;
import com.jivesoftware.os.amza.service.ring.AmzaRingWriter;
import com.jivesoftware.os.amza.service.stats.AmzaStats;
import com.jivesoftware.os.amza.service.storage.PartitionPropertyMarshaller;
import com.jivesoftware.os.amza.service.storage.binary.RowIOProvider;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class MiruAmzaServiceInitializer {

    public AmzaService initialize(Deployable deployable,
        String routesHost,
        int routesPort,
        String connectionsHealthEndpoint,
        int instanceId,
        String instanceKey,
        String serviceName,
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

        SnowflakeIdPacker idPacker = new SnowflakeIdPacker();
        TimestampedOrderIdProvider orderIdProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(instanceId),
            idPacker,
            new JiveEpochTimestampProvider());

        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(SerializationFeature.INDENT_OUTPUT, false);

        AmzaStats amzaStats = new AmzaStats();
        RowsTakerFactory rowsTakerFactory = () -> new HttpRowsTaker(amzaStats);
        AvailableRowsTaker availableRowsTaker = new HttpAvailableRowsTaker(amzaStats);

        AmzaServiceConfig amzaServiceConfig = new AmzaServiceConfig();
        amzaServiceConfig.workingDirectories = config.getWorkingDirectories().split(",");
        amzaServiceConfig.checkIfCompactionIsNeededIntervalInMillis = config.getCheckIfCompactionIsNeededIntervalInMillis();
        amzaServiceConfig.deltaStripeCompactionIntervalInMillis = config.getDeltaStripeCompactionIntervalInMillis();
        amzaServiceConfig.compactTombstoneIfOlderThanNMillis = config.getCompactTombstoneIfOlderThanNMillis();
        amzaServiceConfig.corruptionParanoiaFactor = config.getCorruptionParanoiaFactor();
        amzaServiceConfig.maxUpdatesBeforeDeltaStripeCompaction = config.getMaxUpdatesBeforeDeltaStripeCompaction();
        amzaServiceConfig.numberOfDeltaStripes = amzaServiceConfig.workingDirectories.length;
        amzaServiceConfig.numberOfCompactorThreads = config.getNumberOfCompactorThreads();
        amzaServiceConfig.numberOfTakerThreads = config.getNumberOfTakerThreads();
        amzaServiceConfig.hardFsync = config.getHardFsync();

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

        AmzaService amzaService = new EmbeddedAmzaServiceInitializer().initialize(amzaServiceConfig,
            amzaStats,
            sickThreads,
            ringMember,
            ringHost,
            orderIdProvider,
            idPacker,
            partitionPropertyMarshaller,
            (WALIndexProviderRegistry indexProviderRegistry, RowIOProvider<?> ephemeralRowIOProvider, RowIOProvider<?> persistentRowIOProvider) -> {
                String[] walIndexDirs = config.getIndexDirectories().split(",");
                indexProviderRegistry.register("berkeleydb", new BerkeleyDBWALIndexProvider(walIndexDirs, walIndexDirs.length), persistentRowIOProvider);
            },
            availableRowsTaker,
            rowsTakerFactory,
            Optional.<TakeFailureListener>absent(),
            allRowChanges);

        amzaService.start();

        System.out.println("-----------------------------------------------------------------------");
        System.out.println("|      Amza Service Online");
        System.out.println("-----------------------------------------------------------------------");

        HttpDeliveryClientHealthProvider clientHealthProvider = new HttpDeliveryClientHealthProvider(instanceKey,
            HttpRequestHelperUtils.buildRequestHelper(routesHost, routesPort),
            connectionsHealthEndpoint, 5_000, 100);

        TenantRoutingHttpClientInitializer<String> tenantRoutingHttpClientInitializer = new TenantRoutingHttpClientInitializer<>();
        TenantAwareHttpClient<String> httpClient = tenantRoutingHttpClientInitializer.initialize(
            deployable.getTenantRoutingProvider().getConnections(serviceName, "main"),
            clientHealthProvider,
            10,
            10_000); // TODO expose to conf

        AmzaClientProvider<HttpClient, HttpClientException> clientProvider = new AmzaClientProvider<>(
            new HttpPartitionClientFactory(),
            new HttpPartitionHostsProvider(httpClient, mapper),
            new RingHostHttpClientProvider(httpClient),
            Executors.newCachedThreadPool(),
            10_000); //TODO expose to conf

        new AmzaUIInitializer().initialize(clusterName, ringHost, amzaService, clientProvider, amzaStats, new AmzaUIInitializer.InjectionCallback() {

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

        if (clusterName != null && multicastPort > 0) {
            AmzaDiscovery amzaDiscovery = new AmzaDiscovery(amzaService.getRingReader(),
                amzaService.getRingWriter(),
                clusterName,
                multicastGroup,
                multicastPort);
            amzaDiscovery.start();
            System.out.println("-----------------------------------------------------------------------");
            System.out.println("|      Amza Service Discovery Online");
            System.out.println("-----------------------------------------------------------------------");
        } else {

            System.out.println("-----------------------------------------------------------------------");
            System.out.println("|     Amza Service is in routing bird Discovery mode.  No cluster name was specified or discovery port not set");
            System.out.println("-----------------------------------------------------------------------");
            RoutingBirdAmzaDiscovery routingBirdAmzaDiscovery = new RoutingBirdAmzaDiscovery(deployable,
                serviceName,
                amzaService,
                config.getDiscoveryIntervalMillis());
            routingBirdAmzaDiscovery.start();
        }
        return amzaService;
    }

    static class RoutingBirdAmzaDiscovery implements Runnable {

        private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
        private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);

        private final Deployable deployable;
        private final String serviceName;
        private final AmzaService amzaService;
        private final long discoveryIntervalMillis;

        public RoutingBirdAmzaDiscovery(Deployable deployable, String serviceName, AmzaService amzaService, long discoveryIntervalMillis) {
            this.deployable = deployable;
            this.serviceName = serviceName;
            this.amzaService = amzaService;
            this.discoveryIntervalMillis = discoveryIntervalMillis;
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
                TenantsServiceConnectionDescriptorProvider connections = tenantRoutingProvider.getConnections(serviceName, "main");
                ConnectionDescriptors selfConnections = connections.getConnections("");
                for (ConnectionDescriptor connectionDescriptor : selfConnections.getConnectionDescriptors()) {

                    InstanceDescriptor routingInstanceDescriptor = connectionDescriptor.getInstanceDescriptor();
                    RingMember routingRingMember = new RingMember(
                        Strings.padStart(String.valueOf(routingInstanceDescriptor.instanceName), 5, '0') + "_" + routingInstanceDescriptor.instanceKey);

                    HostPort hostPort = connectionDescriptor.getHostPort();
                    AmzaRingStoreWriter ringWriter = amzaService.getRingWriter();
                    ringWriter.register(routingRingMember, new RingHost(hostPort.getHost(), hostPort.getPort()), -1);
                    ringWriter.addRingMember(AmzaRingReader.SYSTEM_RING, routingRingMember);

                }
            } catch (Exception x) {
                LOG.warn("Failed while calling routing bird discovery.", x);
            }
        }
    }
}
