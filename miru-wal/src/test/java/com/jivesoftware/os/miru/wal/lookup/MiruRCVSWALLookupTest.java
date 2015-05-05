package com.jivesoftware.os.miru.wal.lookup;

import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivityFactory;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.rcvs.inmemory.InMemoryRowColumnValueStore;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 *
 */
public class MiruRCVSWALLookupTest {

    @Test
    public void testRangeMinMax() throws Exception {
        MiruTenantId tenantId = new MiruTenantId("test".getBytes());
        final AtomicLong clockTimestamp = new AtomicLong(0);
        MiruPartitionedActivityFactory factory = new MiruPartitionedActivityFactory(() -> clockTimestamp.incrementAndGet());
        MiruRCVSWALLookup walLookup = new MiruRCVSWALLookup(new InMemoryRowColumnValueStore(), new InMemoryRowColumnValueStore());
        walLookup.add(tenantId, Arrays.asList(
            buildActivity(tenantId, factory, 0, 1001L), // clock 1
            buildActivity(tenantId, factory, 0, 1002L), // clock 2
            buildActivity(tenantId, factory, 0, 1003L), // clock 3
            buildActivity(tenantId, factory, 0, 1004L), // clock 4
            buildActivity(tenantId, factory, 0, 1005L), // clock 5
            buildActivity(tenantId, factory, 1, 2001L), // clock 6
            buildActivity(tenantId, factory, 1, 2002L), // clock 7
            buildActivity(tenantId, factory, 1, 2003L), // clock 8
            buildActivity(tenantId, factory, 1, 2004L), // clock 9
            buildActivity(tenantId, factory, 1, 2005L) // clock 10
        ));

        walLookup.streamRanges(tenantId, (partitionId, type, timestamp) -> {
            if (partitionId.getId() == 0 && type == MiruWALLookup.RangeType.clockMin) {
                assertEquals(timestamp, 1);
            }
            if (partitionId.getId() == 0 && type == MiruWALLookup.RangeType.clockMax) {
                assertEquals(timestamp, 5);
            }
            if (partitionId.getId() == 0 && type == MiruWALLookup.RangeType.orderIdMin) {
                assertEquals(timestamp, 1001L);
            }
            if (partitionId.getId() == 0 && type == MiruWALLookup.RangeType.orderIdMax) {
                assertEquals(timestamp, 1005L);
            }
            if (partitionId.getId() == 1 && type == MiruWALLookup.RangeType.clockMin) {
                assertEquals(timestamp, 6);
            }
            if (partitionId.getId() == 1 && type == MiruWALLookup.RangeType.clockMax) {
                assertEquals(timestamp, 10);
            }
            if (partitionId.getId() == 1 && type == MiruWALLookup.RangeType.orderIdMin) {
                assertEquals(timestamp, 2001L);
            }
            if (partitionId.getId() == 1 && type == MiruWALLookup.RangeType.orderIdMax) {
                assertEquals(timestamp, 2005L);
            }
            return true;
        });
    }

    private MiruPartitionedActivity buildActivity(MiruTenantId tenantId, MiruPartitionedActivityFactory factory, int partitionId, long timestamp) {
        return factory.activity(0, MiruPartitionId.of(partitionId), 0, new MiruActivity(tenantId, timestamp, new String[0], 0,
            Collections.<String, List<String>>emptyMap(), Collections.<String, List<String>>emptyMap()));
    }
}