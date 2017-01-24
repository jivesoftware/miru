package com.jivesoftware.os.miru.wal.deployable;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.io.Files;
import com.jivesoftware.os.amza.api.ring.RingHost;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.embed.EmbedAmzaServiceInitializer.Lifecycle;
import com.jivesoftware.os.amza.service.EmbeddedClientProvider;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.miru.amza.MiruAmzaServiceConfig;
import com.jivesoftware.os.miru.amza.MiruAmzaServiceInitializer;
import com.jivesoftware.os.miru.amza.NoOpClientHealth;
import com.jivesoftware.os.miru.api.HostPortProvider;
import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.marshall.JacksonJsonObjectTypeMarshaller;
import com.jivesoftware.os.miru.api.marshall.MiruVoidByte;
import com.jivesoftware.os.miru.api.wal.RCVSCursor;
import com.jivesoftware.os.miru.api.wal.RCVSSipCursor;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.cluster.MiruRegistryClusterClient;
import com.jivesoftware.os.miru.cluster.MiruReplicaSetDirector;
import com.jivesoftware.os.miru.cluster.amza.AmzaClusterRegistry;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.miru.ui.MiruSoyRendererInitializer;
import com.jivesoftware.os.miru.ui.MiruSoyRendererInitializer.MiruSoyRendererConfig;
import com.jivesoftware.os.miru.wal.MiruWALDirector;
import com.jivesoftware.os.miru.wal.RCVSWALDirector;
import com.jivesoftware.os.miru.wal.RCVSWALInitializer;
import com.jivesoftware.os.miru.wal.activity.rcvs.RCVSActivityWALReader;
import com.jivesoftware.os.miru.wal.activity.rcvs.RCVSActivityWALWriter;
import com.jivesoftware.os.miru.wal.lookup.MiruWALLookup;
import com.jivesoftware.os.miru.wal.lookup.RCVSWALLookup;
import com.jivesoftware.os.miru.wal.readtracking.rcvs.RCVSReadTrackingWALReader;
import com.jivesoftware.os.miru.wal.readtracking.rcvs.RCVSReadTrackingWALWriter;
import com.jivesoftware.os.rcvs.inmemory.InMemoryRowColumnValueStoreInitializer;
import com.jivesoftware.os.routing.bird.deployable.Deployable;
import com.jivesoftware.os.routing.bird.health.api.HealthCheckRegistry;
import com.jivesoftware.os.routing.bird.health.api.HealthChecker;
import com.jivesoftware.os.routing.bird.health.api.HealthFactory;
import com.jivesoftware.os.routing.bird.shared.TenantRoutingProvider;
import java.io.File;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.merlin.config.BindInterfaceToConfiguration;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;

/**
 * @author jonathan.colt
 */
public class MiruWALUIServiceNGTest {

    private MiruWALUIService service;
    private MiruTenantId tenantId;
    private MiruPartitionId partitionId;

    @BeforeClass
    public void before() throws Exception {
        HealthFactory.initialize(BindInterfaceToConfiguration::bindDefault,
            new HealthCheckRegistry() {

                @Override
                public void register(HealthChecker healthChecker) {
                }

                @Override
                public void unregister(HealthChecker healthChecker) {
                    throw new UnsupportedOperationException("Not supported yet.");
                }
            });

        tenantId = new MiruTenantId("test1".getBytes());
        partitionId = MiruPartitionId.of(0);

        MiruSoyRendererConfig config = BindInterfaceToConfiguration.bindDefault(MiruSoyRendererConfig.class);
        config.setPathToSoyResources("src/main/home/resources/soy");

        InMemoryRowColumnValueStoreInitializer rowColumnValueStoreInitializer = new InMemoryRowColumnValueStoreInitializer();
        ObjectMapper mapper = new ObjectMapper();

        RCVSWALInitializer.RCVSWAL wal = new RCVSWALInitializer().initialize("test", rowColumnValueStoreInitializer, mapper);
        wal.getWALLookupTable().add(MiruVoidByte.INSTANCE, tenantId, partitionId, System.currentTimeMillis(), null, null);
        wal.getWriterPartitionRegistry().add(MiruVoidByte.INSTANCE, tenantId, 1, MiruPartitionId.of(0), null, null);

        HostPortProvider hostPortProvider = host -> 10_000;

        RCVSActivityWALWriter activityWALWriter = new RCVSActivityWALWriter(wal.getActivityWAL(), wal.getActivitySipWAL(), null);
        RCVSActivityWALReader activityWALReader = new RCVSActivityWALReader(hostPortProvider, wal.getActivityWAL(), wal.getActivitySipWAL(), null);
        RCVSReadTrackingWALWriter readTrackingWALWriter = new RCVSReadTrackingWALWriter(wal.getReadTrackingWAL(), wal.getReadTrackingSipWAL());
        RCVSReadTrackingWALReader readTrackingWALReader = new RCVSReadTrackingWALReader(hostPortProvider,
            wal.getReadTrackingWAL(),
            wal.getReadTrackingSipWAL());
        MiruWALLookup walLookup = new RCVSWALLookup(wal.getWALLookupTable());

        File amzaDataDir = Files.createTempDir();

        RingMember ringMember = new RingMember("testInstance");
        RingHost ringHost = new RingHost("datacenter", "rack", "localhost", 10000);
        SnowflakeIdPacker idPacker = new SnowflakeIdPacker();
        TimestampedOrderIdProvider orderIdProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(1),
            idPacker,
            new JiveEpochTimestampProvider());
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(SerializationFeature.INDENT_OUTPUT, false);

        MiruAmzaServiceConfig acrc = BindInterfaceToConfiguration.bindDefault(MiruAmzaServiceConfig.class);
        acrc.setWorkingDirectories(amzaDataDir.getAbsolutePath());
        Deployable deployable = new Deployable(new String[0]);
        Lifecycle amzaLifecycle = new MiruAmzaServiceInitializer().initialize(deployable,
            connectionDescriptor -> new NoOpClientHealth(),
            1,
            "instanceKey",
            "serviceName",
            "datacenter",
            "rack",
            "localhost",
            10000,
            false,
            null,
            acrc,
            false,
            1,
            rowsChanged -> {
            });

        EmbeddedClientProvider clientProvider = new EmbeddedClientProvider(amzaLifecycle.amzaService);
        MiruClusterRegistry clusterRegistry = new AmzaClusterRegistry(amzaLifecycle.amzaService,
            clientProvider,
            10_000L,
            new JacksonJsonObjectTypeMarshaller<>(MiruSchema.class, mapper),
            3,
            TimeUnit.HOURS.toMillis(1),
            TimeUnit.HOURS.toMillis(1),
            TimeUnit.DAYS.toMillis(365),
            TimeUnit.DAYS.toMillis(730));
        MiruRegistryClusterClient clusterClient = new MiruRegistryClusterClient(clusterRegistry, new MiruReplicaSetDirector(orderIdProvider, clusterRegistry,
            stream -> {
            }, false));
        RCVSWALDirector director = new RCVSWALDirector(walLookup,
            activityWALReader,
            activityWALWriter,
            readTrackingWALReader,
            readTrackingWALWriter,
            clusterClient);

        MiruSoyRenderer renderer = new MiruSoyRendererInitializer().initialize(config);
        TenantRoutingProvider tenantRoutingProvider = new TenantRoutingProvider(Executors.newScheduledThreadPool(1), "1", (cdr, erg) -> null);
        service = new MiruWALUIServiceInitializer().initialize("cluster",
            1,
            renderer,
            null,
            tenantRoutingProvider,
            director,
            new MiruWALDirector(walLookup, director, activityWALReader, activityWALWriter, clusterClient),
            director,
            null,
            activityWALReader,
            new MiruStats());
    }

    @Test
    public void testRenderActivityWALWithTenant() throws Exception {
        String rendered = service.renderActivityWALWithTenant(tenantId);
        assertTrue(rendered.contains(tenantId.toString()));
        assertTrue(rendered.contains("/ui/activity/" + tenantId + "/" + partitionId + "/rcvs#focus"));
    }

}
