package com.jivesoftware.os.miru.service.partition;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.partition.MiruHostedPartition;
import com.jivesoftware.os.miru.plugin.solution.MiruSolution;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class MiruHostedPartitionComparisonTest {

    private final int windowSize = 1_000;
    private final int percentile = 95;
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
        MiruHostedPartition<Void> p1 = mockPartition(49_601);
        MiruHostedPartition<Void> p2 = mockPartition(49_602);

        ConcurrentSkipListMap<PartitionAndHost, MiruHostedPartition<Void>> skipList = new ConcurrentSkipListMap<>();
        skipList.put(new PartitionAndHost(p1.getPartitionId(), p1.getCoord().host), p1);
        skipList.put(new PartitionAndHost(p2.getPartitionId(), p2.getCoord().host), p2);

        // with no data, ordering is unaffected
        List<MiruHostedPartition<Void>> ordered = partitionComparison.orderPartitions(tenantId, partitionId, queryKey, skipList.values());
        assertEquals(ordered.get(0).getCoord(), p1.getCoord());
        assertEquals(ordered.get(1).getCoord(), p2.getCoord());

        timestamper.set(0);
        partitionComparison.analyzeSolutions(Collections.singletonList(new MiruSolution(p2.getCoord(), 0, 0,
            Collections.<MiruPartitionCoord>emptyList(), null)), queryKey);

        // p2 answered, so now it's on top
        ordered = partitionComparison.orderPartitions(tenantId, partitionId, queryKey, skipList.values());
        assertEquals(ordered.get(0).getCoord(), p2.getCoord());
        assertEquals(ordered.get(1).getCoord(), p1.getCoord());

        timestamper.set(1);
        partitionComparison.analyzeSolutions(Collections.singletonList(new MiruSolution(p1.getCoord(), 0, 0, 
            Collections.<MiruPartitionCoord>emptyList(), null)), queryKey);

        // p1 answered, so now it's on top
        ordered = partitionComparison.orderPartitions(tenantId, partitionId, queryKey, skipList.values());
        assertEquals(ordered.get(0).getCoord(), p1.getCoord());
        assertEquals(ordered.get(1).getCoord(), p2.getCoord());
    }

    @Test
    public void testSuggestedTimeout() throws Exception {
        List<MiruSolution> solutions = Lists.newArrayList();
        for (int i = 1; i <= 100; i++) {
            MiruPartitionCoord coord = new MiruPartitionCoord(tenantId, partitionId, new MiruHost("localhost", 49_600 + i));
            solutions.add(new MiruSolution(coord, i, i, Collections.<MiruPartitionCoord>emptyList(), null));
        }
        partitionComparison.analyzeSolutions(solutions, queryKey);
        assertEquals(partitionComparison.suggestTimeout(tenantId, partitionId, queryKey).get().longValue(), percentile);
    }

    private MiruHostedPartition<Void> mockPartition(int port) {
        MiruHostedPartition<Void> partition = mock(MiruHostedPartition.class);
        when(partition.getCoord()).thenReturn(new MiruPartitionCoord(tenantId, partitionId, new MiruHost("localhost", port)));
        when(partition.getPartitionId()).thenReturn(partitionId);
        when(partition.isLocal()).thenReturn(true);
        when(partition.getStorage()).thenReturn(MiruBackingStorage.memory);
        return partition;
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
