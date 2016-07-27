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
import com.jivesoftware.os.amza.embed.EmbedAmzaServiceInitializer;
import com.jivesoftware.os.amza.embed.EmbedAmzaServiceInitializer.Lifecycle;
import com.jivesoftware.os.amza.lab.pointers.LABPointerIndexConfig;
import com.jivesoftware.os.amza.lab.pointers.LABPointerIndexWALIndexProvider;
import com.jivesoftware.os.amza.service.AmzaInstance;
import com.jivesoftware.os.amza.service.AmzaRingStoreWriter;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.service.AmzaServiceInitializer.AmzaServiceConfig;
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

        SnowflakeIdPacker idPacker = new SnowflakeIdPacker();
        JiveEpochTimestampProvider timestampProvider = new JiveEpochTimestampProvider();

        AmzaStats amzaStats = new AmzaStats();
        BAInterner baInterner = new BAInterner();

        AmzaServiceConfig amzaServiceConfig = new AmzaServiceConfig();
        amzaServiceConfig.workingDirectories = config.getWorkingDirectories().split(",");
        amzaServiceConfig.checkIfCompactionIsNeededIntervalInMillis = config.getCheckIfCompactionIsNeededIntervalInMillis();
        amzaServiceConfig.deltaStripeCompactionIntervalInMillis = config.getDeltaStripeCompactionIntervalInMillis();
        amzaServiceConfig.deltaMaxValueSizeInIndex = config.getDeltaMaxValueSizeInIndex();
        amzaServiceConfig.corruptionParanoiaFactor = config.getCorruptionParanoiaFactor();
        amzaServiceConfig.maxUpdatesBeforeDeltaStripeCompaction = config.getMaxUpdatesBeforeDeltaStripeCompaction();
        amzaServiceConfig.numberOfTakerThreads = config.getNumberOfTakerThreads();
        amzaServiceConfig.hardFsync = config.getHardFsync();
        amzaServiceConfig.takeSlowThresholdInMillis = config.getTakeSlowThresholdInMillis();
        amzaServiceConfig.rackDistributionEnabled = config.getRackDistributionEnabled();

        LABPointerIndexConfig amzaLabConfig = deployable.config(LABPointerIndexConfig.class);

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

        Lifecycle lifecycle = new EmbedAmzaServiceInitializer().initialize(deployable,
            routesHost,
            routesPort,
            connectionsHealthEndpoint,
            instanceId,
            instanceKey,
            serviceName,
            datacenterName,
            rackName,
            hostName,
            port,
            clusterName,
            amzaServiceConfig,
            amzaLabConfig,
            amzaStats,
            baInterner,
            idPacker,
            timestampProvider,
            blacklistRingMembers,
            true,
            false,
            allRowChanges);
        AmzaService amzaService = lifecycle.amzaService;

        lifecycle.startAmzaService();

        if (useAmzaDiscovery) {
            lifecycle.startRoutingBirdAmzaDiscovery();
        }

        return amzaService;
    }
}
