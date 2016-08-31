package com.jivesoftware.os.miru.plugin.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.jivesoftware.os.amza.api.DeltaOverCapacityException;
import com.jivesoftware.os.amza.api.partition.Consistency;
import com.jivesoftware.os.amza.api.partition.Durability;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.service.EmbeddedClientProvider;
import com.jivesoftware.os.amza.service.Partition;
import com.jivesoftware.os.miru.amza.MiruAmzaServiceConfig;
import com.jivesoftware.os.miru.amza.MiruAmzaServiceInitializer;
import com.jivesoftware.os.miru.amza.NoOpClientHealth;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivityFactory;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.wal.AmzaCursor;
import com.jivesoftware.os.miru.api.wal.AmzaSipCursor;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.miru.api.wal.MiruWALEntry;
import com.jivesoftware.os.miru.service.MiruServiceConfig;
import com.jivesoftware.os.miru.wal.AmzaWALUtil;
import com.jivesoftware.os.miru.wal.MiruWALDirector;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReader;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALWriter;
import com.jivesoftware.os.miru.wal.activity.amza.AmzaActivityWALReader;
import com.jivesoftware.os.miru.wal.activity.amza.AmzaActivityWALWriter;
import com.jivesoftware.os.routing.bird.deployable.Deployable;
import com.jivesoftware.os.routing.bird.health.api.HealthCheckRegistry;
import com.jivesoftware.os.routing.bird.health.api.HealthChecker;
import com.jivesoftware.os.routing.bird.health.api.HealthFactory;
import com.jivesoftware.os.routing.bird.shared.HostPort;
import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.merlin.config.BindInterfaceToConfiguration;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 *
 */
public class AmzaWALTest {

    MiruTenantId tenantId = new MiruTenantId("test".getBytes());
    MiruPartitionId partitionId = MiruPartitionId.of(0);

    @Test(enabled = false, description = "Need to factor out ring size of WAL")
    public void testStreamWAL() throws Exception {

        HealthFactory.initialize(
            BindInterfaceToConfiguration::bindDefault,
            new HealthCheckRegistry() {
                @Override
                public void register(HealthChecker healthChecker) {
                }

                @Override
                public void unregister(HealthChecker healthChecker) {
                }
            });

        MiruServiceConfig config = BindInterfaceToConfiguration.bindDefault(MiruServiceConfig.class);
        config.setDefaultFailAfterNMillis(TimeUnit.HOURS.toMillis(1));
        config.setPersistentMergeChitCount(10_000);
        config.setTransientMergeChitCount(10_000);

        Logger rootLogger = LogManager.getRootLogger();
        if (rootLogger instanceof org.apache.logging.log4j.core.Logger) {
            LoggerContext context = ((org.apache.logging.log4j.core.Logger) rootLogger).getContext();
            LoggerConfig loggerConfig = context.getConfiguration().getLoggerConfig("");
            loggerConfig.setLevel(Level.WARN);
        }

        ObjectMapper mapper = new ObjectMapper();

        File amzaDataDir = Files.createTempDir();
        MiruAmzaServiceConfig acrc = BindInterfaceToConfiguration.bindDefault(MiruAmzaServiceConfig.class);
        acrc.setWorkingDirectories(amzaDataDir.getAbsolutePath());
        acrc.setMaxUpdatesBeforeDeltaStripeCompaction(100_000);
        Deployable deployable = new Deployable(new String[0]);
        AmzaService amzaService = new MiruAmzaServiceInitializer().initialize(deployable,
            connectionDescriptor -> new NoOpClientHealth(),
            1,
            "instanceKey",
            "serviceName",
            "datacenter",
            "rack",
            "localhost",
            10000,
            null,
            acrc,
            false,
            rowsChanged -> {
            });
        EmbeddedClientProvider amzaClientProvider = new EmbeddedClientProvider(amzaService);
        //WALStorageDescriptor storageDescriptor = new WALStorageDescriptor(false, new PrimaryIndexDescriptor("berkeleydb", 0, false, null),
        //    null, 1000, 1000);

        AmzaWALUtil amzaWALUtil = new AmzaWALUtil(amzaService, amzaClientProvider,
            new PartitionProperties(Durability.fsync_async, 0, 0, 0, 0, 0, 0, 0, 0, false, Consistency.leader_quorum, true, false, false,
                RowType.snappy_primary, "lab", -1, null, -1, -1),
            new PartitionProperties(Durability.fsync_async, 0, 0, 0, 0, 0, 0, 0, 0, false, Consistency.leader_quorum, true, false, false,
                RowType.snappy_primary, "lab", -1, null, -1, -1),
            new PartitionProperties(Durability.fsync_async, 0, 0, 0, 0, 0, 0, 0, 0, false, Consistency.quorum, true, false, false,
                RowType.primary, "lab", -1, null, -1, -1),
            3,
            10_000,
            3,
            10_000);
        MiruActivityWALWriter activityWALWriter = new AmzaActivityWALWriter(amzaWALUtil, 0, mapper);
        MiruActivityWALReader<AmzaCursor, AmzaSipCursor> activityWALReader = new AmzaActivityWALReader(amzaWALUtil, mapper);

        HostPort[] routingGroup = activityWALReader.getRoutingGroup(tenantId, partitionId);
        assertNotNull(routingGroup);

        MiruPartitionedActivityFactory partitionedActivityFactory = new MiruPartitionedActivityFactory();
        int batchSize = 1_000;
        int numBatches = 500;
        for (int i = 0; i < numBatches; i++) {
            List<MiruPartitionedActivity> partitionedActivities = Lists.newArrayListWithCapacity(batchSize + 1);
            for (int j = 0; j < batchSize; j++) {
                partitionedActivities.add(partitionedActivityFactory.activity(1,
                    partitionId,
                    i * batchSize + j,
                    new MiruActivity(tenantId,
                        i * batchSize + j,
                        System.currentTimeMillis(),
                        false,
                        new String[0],
                        Maps.newHashMap(),
                        Maps.newHashMap())));
            }
            partitionedActivities.add(partitionedActivityFactory.begin(1, partitionId, tenantId, (i + 1) * batchSize - 1));
            while (true) {
                try {
                    activityWALWriter.write(tenantId, partitionId, partitionedActivities);
                    break;
                } catch (DeltaOverCapacityException e) {
                    System.out.println("Waiting for delta to merge...");
                    Thread.sleep(1_000);
                }
            }
        }

        System.out.println("Merging...");
        amzaService.mergeAllDeltas(true);
        System.out.println("Merged!");
        //amzaService.compactAllTombstones();

        for (PartitionName partitionName : amzaService.getMemberPartitionNames()) {
            Partition partition = amzaService.getPartition(partitionName);
            System.out.println("Count: " + partitionName + " = " + partition.count());
        }

        byte[] partitionNameBytes = "activityWAL-test-0".getBytes(Charsets.UTF_8);
        Partition partition = amzaService.getPartition(new PartitionName(false, partitionNameBytes, partitionNameBytes));
        System.out.println("Count: " + partition.count());
        //assertEquals(partition.count(), batchSize * numBatches);

        MiruWALDirector<AmzaCursor, AmzaSipCursor> walDirector = new MiruWALDirector<>(null, activityWALReader, activityWALWriter, null, null, null);

        AmzaCursor cursor = null;
        int count = 0;
        while (true) {
            MiruWALClient.StreamBatch<MiruWALEntry, AmzaCursor> streamBatch = walDirector.getActivity(tenantId, partitionId, cursor, 317);
            count += streamBatch.activities.size();
            cursor = streamBatch.cursor;
            System.out.println("Streamed " + streamBatch.activities.size() + " " + count);
            if (streamBatch.activities.size() == 0) {
                break;
            }
        }
        assertEquals(count, batchSize * numBatches);
    }
}
