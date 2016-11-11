package com.jivesoftware.os.miru.sync.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.api.PartitionClient;
import com.jivesoftware.os.amza.api.PartitionClientProvider;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.wal.KeyUtil;
import com.jivesoftware.os.amza.client.aquarium.AmzaClientAquariumProvider;
import com.jivesoftware.os.amza.client.test.InMemoryPartitionClient;
import com.jivesoftware.os.aquarium.AquariumStats;
import com.jivesoftware.os.aquarium.Member;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivityFactory;
import com.jivesoftware.os.miru.api.activity.TimeAndVersion;
import com.jivesoftware.os.miru.api.activity.schema.DefaultMiruSchemaDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchemaProvider;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.sync.MiruSyncClient;
import com.jivesoftware.os.miru.api.topology.NamedCursor;
import com.jivesoftware.os.miru.api.wal.AmzaCursor;
import com.jivesoftware.os.miru.api.wal.AmzaSipCursor;
import com.jivesoftware.os.miru.api.wal.MiruActivityWALStatus;
import com.jivesoftware.os.miru.api.wal.MiruVersionedActivityLookupEntry;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.miru.api.wal.MiruWALEntry;
import com.jivesoftware.os.miru.sync.deployable.MiruSyncSender.ProgressType;
import com.jivesoftware.os.routing.bird.shared.HostPort;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.jivesoftware.os.miru.sync.deployable.MiruSyncSender.ProgressType.forward;
import static com.jivesoftware.os.miru.sync.deployable.MiruSyncSender.ProgressType.initial;
import static com.jivesoftware.os.miru.sync.deployable.MiruSyncSender.ProgressType.reverse;

/**
 *
 */
public class MiruSyncSenderTest {
    @Test
    public void testProgress() throws Exception {
        MiruTenantId tenantId = new MiruTenantId("tenant1".getBytes(StandardCharsets.UTF_8));

        TimestampedOrderIdProvider orderIdProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(1),
            new SnowflakeIdPacker(),
            new JiveEpochTimestampProvider());
        PartitionClientProvider partitionClientProvider = new InMemoryPartitionClientProvider(orderIdProvider);

        AmzaClientAquariumProvider amzaClientAquariumProvider = new AmzaClientAquariumProvider(new AquariumStats(),
            "test",
            partitionClientProvider,
            orderIdProvider,
            new Member("member1".getBytes(StandardCharsets.UTF_8)),
            count -> count == 1,
            member -> true,
            128, //TODO config
            128, //TODO config
            5_000L, //TODO config
            100L, //TODO config
            60_000L, //TODO config
            10_000L, //TODO config
            Executors.newSingleThreadExecutor(),
            100L, //TODO config
            1_000L, //TODO config
            10_000L); //TODO config

        AtomicInteger largestPartitionId = new AtomicInteger(10);
        int initialId = largestPartitionId.get();

        int[] reverseSyncedActivity = new int[1];
        int[] reverseSyncedBoundary = new int[1];
        int[] forwardSyncedActivity = new int[1];
        int[] forwardSyncedBoundary = new int[1];
        MiruSyncClient syncClient = new MiruSyncClient() {
            @Override
            public void writeActivity(MiruTenantId tenantId,
                MiruPartitionId partitionId,
                List<MiruPartitionedActivity> partitionedActivities) throws Exception {
                for (MiruPartitionedActivity partitionedActivity : partitionedActivities) {
                    if (partitionId.getId() < initialId) {
                        if (partitionedActivity.type.isActivityType()) {
                            reverseSyncedActivity[0]++;
                        } else if (partitionedActivity.type.isBoundaryType()) {
                            reverseSyncedBoundary[0]++;
                        }
                    } else {
                        if (partitionedActivity.type.isActivityType()) {
                            forwardSyncedActivity[0]++;
                        } else if (partitionedActivity.type.isBoundaryType()) {
                            forwardSyncedBoundary[0]++;
                        }
                    }
                }
            }

            @Override
            public void registerSchema(MiruTenantId tenantId, MiruSchema schema) throws Exception {
            }
        };

        MiruSchema schema = new MiruSchema.Builder("test", 1)
            .setFieldDefinitions(DefaultMiruSchemaDefinition.FIELDS)
            .build();
        MiruSchemaProvider schemaProvider = miruTenantId -> schema;

        TestWALClient testWALClient = new TestWALClient(tenantId, largestPartitionId);
        MiruSyncSender<AmzaCursor, AmzaSipCursor> syncService = new MiruSyncSender<>(amzaClientAquariumProvider,
            1,
            Executors.newSingleThreadExecutor(),
            1,
            100L,
            schemaProvider,
            testWALClient,
            syncClient,
            partitionClientProvider,
            new ObjectMapper(),
            null,
            1_000,
            0,
            -1,
            null,
            AmzaCursor.class);

        amzaClientAquariumProvider.start();
        syncService.start();

        long failAfter = System.currentTimeMillis() + 60_000L;
        int[] progressIds = awaitProgress(tenantId, syncService, reverse, -1, failAfter);

        Assert.assertEquals(progressIds[initial.index], initialId);
        Assert.assertEquals(progressIds[reverse.index], -1);
        Assert.assertEquals(progressIds[forward.index], initialId);
        Assert.assertEquals(reverseSyncedActivity[0], largestPartitionId.get(),
            "Should reverse sync 1 activity each for partitions less than " + largestPartitionId.get());
        Assert.assertEquals(reverseSyncedBoundary[0], 2 * largestPartitionId.get(),
            "Should reverse sync 1 boundary each for partitions less than " + largestPartitionId.get());
        Assert.assertEquals(forwardSyncedActivity[0], 0, "Should not forward sync any activity yet");
        Assert.assertEquals(forwardSyncedBoundary[0], 0, "Should not forward sync any boundary yet");

        reverseSyncedActivity[0] = 0;
        reverseSyncedBoundary[0] = 0;
        largestPartitionId.addAndGet(10);

        progressIds = awaitProgress(tenantId, syncService, forward, largestPartitionId.get(), failAfter);

        Assert.assertEquals(progressIds[initial.index], initialId);
        Assert.assertEquals(progressIds[reverse.index], -1);
        Assert.assertEquals(progressIds[forward.index], largestPartitionId.get());
        Assert.assertEquals(reverseSyncedActivity[0], 0, "Should not reverse sync any additional activity yet");
        Assert.assertEquals(reverseSyncedBoundary[0], 0, "Should not reverse sync any additional boundary yet");
        Assert.assertEquals(forwardSyncedActivity[0], largestPartitionId.get() - initialId,
            "Should forward sync 1 activity each for partitions from " + initialId + " to " + largestPartitionId.get());
        Assert.assertEquals(forwardSyncedActivity[0], largestPartitionId.get() - initialId,
            "Should forward sync 1 boundary each for partitions from " + initialId + " to " + largestPartitionId.get());
    }

    private int[] awaitProgress(MiruTenantId tenantId,
        MiruSyncSender<AmzaCursor, AmzaSipCursor> syncService,
        ProgressType awaitType,
        int awaitValue,
        long failAfter) throws Exception {
        int[] progressIds = { Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE };
        while (true) {
            syncService.streamProgress(tenantId, tenantId, (toTenantId, type, partitionId) -> {
                Assert.assertEquals(toTenantId, tenantId);
                progressIds[type.index] = partitionId;
                return true;
            });
            if (progressIds[awaitType.index] == awaitValue) {
                break;
            }
            if (System.currentTimeMillis() > failAfter) {
                Assert.fail("Timed out awaiting progress");
            }
            Thread.sleep(100L);
        }
        return progressIds;
    }

    private static class InMemoryPartitionClientProvider implements PartitionClientProvider {

        private final OrderIdProvider orderIdProvider;

        private final Map<PartitionName, PartitionClient> clients = Maps.newConcurrentMap();

        public InMemoryPartitionClientProvider(OrderIdProvider orderIdProvider) {
            this.orderIdProvider = orderIdProvider;
        }

        @Override
        public PartitionProperties getProperties(PartitionName partitionName) throws Exception {
            return null;
        }

        @Override
        public PartitionClient getPartition(PartitionName partitionName) throws Exception {
            return clients.computeIfAbsent(partitionName,
                partitionName1 -> new InMemoryPartitionClient(new ConcurrentSkipListMap<>(KeyUtil.lexicographicalComparator()), orderIdProvider));
        }

        @Override
        public PartitionClient getPartition(PartitionName partitionName, int ringSize, PartitionProperties partitionProperties) throws Exception {
            return getPartition(partitionName);
        }
    }

    private static class TestWALClient implements MiruWALClient<AmzaCursor, AmzaSipCursor> {

        private final MiruTenantId testTenantId;
        private final AtomicInteger largestPartitionId;

        private final AtomicLong timestamper = new AtomicLong(1_000);
        private final MiruPartitionedActivityFactory partitionedActivityFactory = new MiruPartitionedActivityFactory(timestamper::incrementAndGet);
        private final NamedCursor continueCursor = new NamedCursor("test", 1L);
        private final NamedCursor stopCursor = new NamedCursor("test", 2L);

        private TestWALClient(MiruTenantId testTenantId, AtomicInteger largestPartitionId) {
            this.testTenantId = testTenantId;
            this.largestPartitionId = largestPartitionId;
        }

        @Override
        public List<MiruTenantId> getAllTenantIds() throws Exception {
            return Collections.singletonList(testTenantId);
        }

        @Override
        public MiruPartitionId getLargestPartitionId(MiruTenantId tenantId) throws Exception {
            return MiruPartitionId.of(largestPartitionId.get());
        }

        @Override
        public MiruActivityWALStatus getActivityWALStatusForTenant(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
            Preconditions.checkArgument(tenantId.equals(testTenantId));
            List<Integer> begins = Collections.singletonList(1);
            List<Integer> ends = (partitionId.getId() < largestPartitionId.get()) ? begins : Collections.emptyList();
            return new MiruActivityWALStatus(partitionId, Collections.emptyList(), begins, ends);
        }

        @Override
        public StreamBatch<MiruWALEntry, AmzaCursor> getActivity(MiruTenantId tenantId,
            MiruPartitionId partitionId,
            AmzaCursor cursor,
            int batchSize) throws Exception {
            List<MiruWALEntry> entries;
            NamedCursor namedCursor = (cursor == null || cursor.cursors == null || cursor.cursors.isEmpty()) ? null : cursor.cursors.get(0);
            if (partitionId.getId() == largestPartitionId.get() || namedCursor != null && namedCursor.compareTo(stopCursor) == 0) {
                entries = Collections.emptyList();
            } else {
                entries = Collections.singletonList(
                    new MiruWALEntry(1L,
                        2L,
                        partitionedActivityFactory.activity(1,
                            partitionId,
                            0,
                            new MiruActivity(tenantId, 1L, 2L, false, null, Collections.emptyMap(), Collections.emptyMap()))));
            }
            return new StreamBatch<>(
                entries,
                new AmzaCursor(Collections.singletonList(entries.isEmpty() ? continueCursor : stopCursor),
                    new AmzaSipCursor(Collections.emptyList(), true)),
                true,
                Collections.emptySet());
        }

        @Override
        public HostPort[] getTenantRoutingGroup(RoutingGroupType routingGroupType, MiruTenantId tenantId) throws Exception {
            return new HostPort[0];
        }

        @Override
        public HostPort[] getTenantPartitionRoutingGroup(RoutingGroupType routingGroupType,
            MiruTenantId tenantId,
            MiruPartitionId partitionId) throws Exception {
            return new HostPort[0];
        }

        @Override
        public HostPort[] getTenantStreamRoutingGroup(RoutingGroupType routingGroupType, MiruTenantId tenantId, MiruStreamId streamId) throws Exception {
            return new HostPort[0];
        }

        @Override
        public void writeActivity(MiruTenantId tenantId, MiruPartitionId partitionId, List<MiruPartitionedActivity> partitionedActivities) throws Exception {

        }

        @Override
        public void writeReadTracking(MiruTenantId tenantId, MiruStreamId streamId, List<MiruPartitionedActivity> partitionedActivities) throws Exception {

        }

        @Override
        public WriterCursor getCursorForWriterId(MiruTenantId tenantId, MiruPartitionId partitionId, int writerId) throws Exception {
            return null;
        }

        @Override
        public long oldestActivityClockTimestamp(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
            return 0;
        }

        @Override
        public List<MiruVersionedActivityLookupEntry> getVersionedEntries(MiruTenantId tenantId,
            MiruPartitionId partitionId,
            Long[] timestamps) throws Exception {
            return null;
        }

        @Override
        public StreamBatch<MiruWALEntry, AmzaSipCursor> sipActivity(MiruTenantId tenantId,
            MiruPartitionId partitionId,
            AmzaSipCursor cursor,
            Set<TimeAndVersion> lastSeen,
            int batchSize) throws Exception {
            return null;
        }

        @Override
        public StreamBatch<MiruWALEntry, AmzaSipCursor> getRead(MiruTenantId tenantId,
            MiruStreamId streamId,
            AmzaSipCursor cursor,
            long oldestEventId,
            int batchSize) throws Exception {
            return null;
        }
    }

    ;
}