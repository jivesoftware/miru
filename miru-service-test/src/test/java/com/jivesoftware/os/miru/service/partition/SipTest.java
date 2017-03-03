package com.jivesoftware.os.miru.service.partition;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.jivesoftware.os.miru.api.HostPortProvider;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartition;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivityFactory;
import com.jivesoftware.os.miru.api.activity.TimeAndVersion;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.field.MiruFieldName;
import com.jivesoftware.os.miru.api.topology.HostHeartbeat;
import com.jivesoftware.os.miru.api.topology.MiruClusterClient;
import com.jivesoftware.os.miru.api.topology.MiruHeartbeatRequest;
import com.jivesoftware.os.miru.api.topology.MiruHeartbeatResponse;
import com.jivesoftware.os.miru.api.topology.MiruIngressUpdate;
import com.jivesoftware.os.miru.api.topology.MiruPartitionStatus;
import com.jivesoftware.os.miru.api.topology.MiruTenantConfig;
import com.jivesoftware.os.miru.api.topology.MiruTopologyResponse;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.miru.api.wal.MiruWALEntry;
import com.jivesoftware.os.miru.api.wal.RCVSSipCursor;
import com.jivesoftware.os.miru.wal.RCVSWALDirector;
import com.jivesoftware.os.miru.wal.RCVSWALInitializer;
import com.jivesoftware.os.miru.wal.activity.rcvs.RCVSActivityWALReader;
import com.jivesoftware.os.miru.wal.activity.rcvs.RCVSActivityWALWriter;
import com.jivesoftware.os.miru.wal.lookup.RCVSWALLookup;
import com.jivesoftware.os.rcvs.inmemory.InMemoryRowColumnValueStoreInitializer;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 *
 */
public class SipTest {

    @Test(enabled = false)
    public void testSipConsistency() throws Exception {
        MiruTenantId tenantId = new MiruTenantId("test".getBytes());
        MiruPartitionId partitionId = MiruPartitionId.of(0);
        RCVSWALInitializer.RCVSWAL rcvsWAL = new RCVSWALInitializer().initialize("test", new InMemoryRowColumnValueStoreInitializer(), new ObjectMapper());
        HostPortProvider hostPortProvider = host -> 10_000;
        RCVSWALDirector walDirector = new RCVSWALDirector(new RCVSWALLookup(rcvsWAL.getWALLookupTable()),
            new RCVSActivityWALReader(hostPortProvider, rcvsWAL.getActivityWAL(), rcvsWAL.getActivitySipWAL(), null),
            new RCVSActivityWALWriter(rcvsWAL.getActivityWAL(), rcvsWAL.getActivitySipWAL(), null),
            null,
            null,
            new NoOpClusterClient());

        ExecutorService executorService = Executors.newCachedThreadPool();
        List<Future<?>> producerFutures = Lists.newArrayList();
        AtomicLong timestamp = new AtomicLong(0);
        for (int i = 0; i < 3; i++) {
            int node = i;
            MiruPartitionedActivityFactory factory = new MiruPartitionedActivityFactory(() -> System.currentTimeMillis() - (node * 5_000));
            producerFutures.add(executorService.submit(() -> {
                try {
                    for (int j = 0; j < Integer.MAX_VALUE; j++) {
                        Map<String, List<String>> fieldsValues = ImmutableMap.of(MiruFieldName.OBJECT_ID.getFieldName(),
                            Collections.singletonList(String.valueOf(j)));
                        MiruActivity activity = new MiruActivity(tenantId,
                            timestamp.incrementAndGet(),
                            0,
                            false,
                            new String[0],
                            fieldsValues,
                            Collections.emptyMap());
                        walDirector.writeActivity(tenantId, partitionId, Collections.singletonList(factory.activity(1, partitionId, j, activity)));
                        Thread.sleep(1);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    Assert.fail();
                }
            }));
        }

        List<LinkedHashSet<Long>> nodeTimestamps = Lists.newArrayList();
        for (int i = 0; i < 3; i++) {
            LinkedHashSet<Long> timestamps = Sets.newLinkedHashSet();
            nodeTimestamps.add(timestamps);
            Random r = new Random();
            int node = i;
            executorService.submit(() -> {
                try {
                    Set<TimeAndVersion> seenLastSip = Sets.newHashSet();
                    while (true) {
                        RCVSSipCursor sipCursor = RCVSSipCursor.INITIAL;
                        CursorAndLastSeen cursorAndLastSeen = sip(tenantId, partitionId, walDirector, sipCursor, seenLastSip, partitionedActivity -> {
                            timestamps.add(partitionedActivity.timestamp);
                        });
                        sipCursor = cursorAndLastSeen.sipCursor;
                        seenLastSip = cursorAndLastSeen.seenLastSip;
                        System.out.println("Node " + node + " has seen " + timestamps.size());
                        Thread.sleep(r.nextInt(5000));
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }

        System.out.println("Running...");
        Thread.sleep(60_000L);

        System.out.println("Shutting down producer.");
        producerFutures.forEach(future -> future.cancel(true));

        System.out.println("Waiting for sips to finish...");
        Thread.sleep(10_000L);
        executorService.shutdownNow();

        for (int i = 0; i < nodeTimestamps.size() - 1; i++) {
            assertEquals(nodeTimestamps.get(i), nodeTimestamps.get(i + 1));
        }
    }

    interface ActivityStream {
        void stream(MiruPartitionedActivity partitionedActivity) throws Exception;
    }

    private static class CursorAndLastSeen {
        private final RCVSSipCursor sipCursor;
        private final Set<TimeAndVersion> seenLastSip;

        public CursorAndLastSeen(RCVSSipCursor sipCursor, Set<TimeAndVersion> seenLastSip) {
            this.sipCursor = sipCursor;
            this.seenLastSip = seenLastSip;
        }
    }

    private CursorAndLastSeen sip(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        RCVSWALDirector walDirector,
        RCVSSipCursor sipCursor,
        Set<TimeAndVersion> seenLastSip,
        ActivityStream stream) throws Exception {

        final MiruSipTracker<RCVSSipCursor> sipTracker = new RCVSSipTracker(100, 10_000, seenLastSip);

        MiruWALClient.StreamBatch<MiruWALEntry, RCVSSipCursor> sippedActivity = walDirector.sipActivity(tenantId, partitionId,
            sipCursor,
            null,
            10_000);

        while (sippedActivity != null) {
            if (Thread.interrupted()) {
                throw new InterruptedException("Interrupted while streaming sip");
            }

            List<MiruPartitionedActivity> partitionedActivities = Lists.newArrayListWithCapacity(sippedActivity.activities.size());
            for (MiruWALEntry e : sippedActivity.activities) {
                long version = e.activity.activity.isPresent() ? e.activity.activity.get().version : 0; // Smells!
                TimeAndVersion timeAndVersion = new TimeAndVersion(e.activity.timestamp, version);

                if (!sipTracker.wasSeenLastSip(timeAndVersion)) {
                    partitionedActivities.add(e.activity);
                }
                sipTracker.addSeenThisSip(timeAndVersion);
                sipTracker.track(e.activity);
            }

            RCVSSipCursor lastCursor = sipCursor;
            sipCursor = deliver(partitionedActivities, sipTracker, sipCursor, sippedActivity.cursor, stream);
            partitionedActivities.clear();

            if (sippedActivity.cursor != null && sippedActivity.cursor.endOfStream()) {
                break;
            } else if (sipCursor == null) {
                break;
            } else if (sipCursor.equals(lastCursor)) {
                break;
            } else if (sippedActivity.endOfWAL) {
                break;
            }

            sippedActivity = walDirector.sipActivity(tenantId, partitionId, sipCursor, null, 10_000);
        }

        return new CursorAndLastSeen(sipCursor, seenLastSip);
    }

    private RCVSSipCursor deliver(List<MiruPartitionedActivity> partitionedActivities,
        MiruSipTracker<RCVSSipCursor> sipTracker,
        RCVSSipCursor sipCursor,
        RCVSSipCursor nextSipCursor,
        ActivityStream stream) throws Exception {

        for (MiruPartitionedActivity partitionedActivity : partitionedActivities) {
            stream.stream(partitionedActivity);
        }

        return sipTracker.suggest(sipCursor, nextSipCursor);
    }

    private static class NoOpClusterClient implements MiruClusterClient {
        @Override
        public List<HostHeartbeat> allhosts() throws Exception {
            return Collections.emptyList();
        }

        @Override
        public MiruSchema getSchema(MiruTenantId tenantId) throws Exception {
            return null;
        }

        @Override
        public List<MiruPartition> partitions(MiruTenantId tenantId) throws Exception {
            return Collections.emptyList();
        }

        @Override
        public List<PartitionRange> getIngressRanges(MiruTenantId tenantId) throws Exception {
            return Collections.emptyList();
        }

        @Override
        public void registerSchema(MiruTenantId tenantId, MiruSchema schema) throws Exception {

        }

        @Override
        public void removeHost(MiruHost host) throws Exception {

        }

        @Override
        public void removeTopology(MiruHost host, MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {

        }

        @Override
        public MiruTenantConfig tenantConfig(MiruTenantId tenantId) throws Exception {
            return null;
        }

        @Override
        public void updateIngress(MiruIngressUpdate ingressUpdate) throws Exception {

        }

        @Override
        public void removeIngress(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {

        }

        @Override
        public void updateLastId(MiruPartitionCoord coord, int lastId) throws Exception {

        }

        @Override
        public void destroyPartition(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {

        }

        @Override
        public List<MiruPartitionStatus> getPartitionStatus(MiruTenantId tenantId, MiruPartitionId largestPartitionId) throws Exception {
            return Collections.emptyList();
        }

        @Override
        public MiruHeartbeatResponse thumpthump(MiruHost host, MiruHeartbeatRequest heartbeatRequest) throws Exception {
            return null;
        }

        @Override
        public MiruTopologyResponse routingTopology(MiruTenantId tenantId) throws Exception {
            return null;
        }
    }
}
