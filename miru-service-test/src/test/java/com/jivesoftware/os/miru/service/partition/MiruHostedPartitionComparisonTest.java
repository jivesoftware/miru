package com.jivesoftware.os.miru.service.partition;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.partition.MiruRoutablePartition;
import com.jivesoftware.os.miru.plugin.solution.MiruSolution;
import com.jivesoftware.os.miru.service.partition.cluster.PartitionAndHost;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class MiruHostedPartitionComparisonTest {

    private final int windowSize = 1_000;
    private final int percentile = 95;
    private final String requestName = "test";
    private final String queryKey = "TestExecuteQuery";

    private MiruHostedPartitionComparison partitionComparison;
    private TestTimestamper timestamper = new TestTimestamper();
    private MiruTenantId tenantId;
    private MiruPartitionId partitionId;

    @BeforeMethod
    public void setUp() throws Exception {
        partitionComparison = new MiruHostedPartitionComparison(windowSize, percentile, timestamper);
        tenantId = new MiruTenantId("test".getBytes(Charsets.UTF_8));
        partitionId = MiruPartitionId.of(0);
    }

    @Test
    public void testOrdering() throws Exception {
        MiruRoutablePartition p1 = create(49_601);
        MiruRoutablePartition p2 = create(49_602);

        ConcurrentSkipListMap<PartitionAndHost, MiruRoutablePartition> skipList = new ConcurrentSkipListMap<>();
        skipList.put(new PartitionAndHost(p1.partitionId, p1.host), p1);
        skipList.put(new PartitionAndHost(p2.partitionId, p2.host), p2);

        // with no data, ordering is unaffected
        List<MiruRoutablePartition> ordered = partitionComparison.orderPartitions(tenantId, partitionId, requestName, queryKey, skipList.values());
        assertEquals(ordered.get(0).host, p1.host);
        assertEquals(ordered.get(1).host, p2.host);

        timestamper.set(0);
        partitionComparison.analyzeSolutions(Collections.singletonList(new MiruSolution(new MiruPartitionCoord(tenantId, partitionId, p2.host), 0, 0,
            Collections.emptyList(), null)), requestName, queryKey);

        // p2 answered, so now it's on top
        ordered = partitionComparison.orderPartitions(tenantId, partitionId, requestName, queryKey, skipList.values());
        assertEquals(ordered.get(0).host, p2.host);
        assertEquals(ordered.get(1).host, p1.host);

        timestamper.set(1);
        partitionComparison.analyzeSolutions(Collections.singletonList(new MiruSolution(new MiruPartitionCoord(tenantId, partitionId, p1.host), 0, 0,
            Collections.emptyList(), null)), requestName, queryKey);

        // p1 answered, so now it's on top
        ordered = partitionComparison.orderPartitions(tenantId, partitionId, requestName, queryKey, skipList.values());
        assertEquals(ordered.get(0).host, p1.host);
        assertEquals(ordered.get(1).host, p2.host);
    }

    @Test
    public void testSuggestedTimeout() throws Exception {
        List<MiruSolution> solutions = Lists.newArrayList();
        for (int i = 1; i <= 100; i++) {
            MiruPartitionCoord coord = new MiruPartitionCoord(tenantId, partitionId, new MiruHost("logicalName_" + (10_000 + i)));
            solutions.add(new MiruSolution(coord, i, i, Collections.emptyList(), null));
        }
        partitionComparison.analyzeSolutions(solutions, requestName, queryKey);
        assertEquals(partitionComparison.suggestTimeout(tenantId, partitionId, requestName, queryKey).get().longValue(), percentile);
    }

    private MiruRoutablePartition create(int port) {
        return new MiruRoutablePartition(new MiruHost("logicalName_" + port), partitionId, true, MiruPartitionState.online, MiruBackingStorage.memory,
            Long.MAX_VALUE);
    }

    private class TestTimestamper implements MiruHostedPartitionComparison.Timestamper {
        private long timestamp;

        public void set(long timestamp) {
            this.timestamp = timestamp;
        }

        @Override
        public long get() {
            return timestamp;
        }
    }
}
