package com.jivesoftware.os.miru.wal.activity;

import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivityFactory;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.wal.RCVSCursor;
import com.jivesoftware.os.miru.api.wal.RCVSSipCursor;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruActivitySipWALColumnKey;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruActivityWALColumnKey;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruActivityWALRow;
import com.jivesoftware.os.miru.wal.activity.rcvs.RCVSActivityWALReader;
import com.jivesoftware.os.miru.wal.activity.rcvs.RCVSActivityWALWriter;
import com.jivesoftware.os.rcvs.api.RowColumnValueStore;
import com.jivesoftware.os.rcvs.inmemory.InMemoryRowColumnValueStore;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class RCVSActivityWALReaderTest {

    @Test
    public void testStream() throws Exception {
        final MiruTenantId tenantId = new MiruTenantId("test".getBytes());
        final MiruPartitionId partitionId = MiruPartitionId.of(0);
        final int batchSize = 10;
        final int totalActivities = 100;
        final long startingTimestamp = 1000;

        RowColumnValueStore<MiruTenantId, MiruActivityWALRow, MiruActivityWALColumnKey, MiruPartitionedActivity, ? extends Exception> activityWAL =
            new InMemoryRowColumnValueStore<>();
        RowColumnValueStore<MiruTenantId, MiruActivityWALRow, MiruActivitySipWALColumnKey, MiruPartitionedActivity, ? extends Exception> activitySipWAL =
            new InMemoryRowColumnValueStore<>();

        RCVSActivityWALWriter activityWALWriter = new RCVSActivityWALWriter(activityWAL, activitySipWAL);
        RCVSActivityWALReader activityWALReader = new RCVSActivityWALReader(host -> 10_000, activityWAL, activitySipWAL);
        MiruPartitionedActivityFactory partitionedActivityFactory = new MiruPartitionedActivityFactory();

        for (int i = 0; i < totalActivities; i++) {
            activityWALWriter.write(tenantId, partitionId, Collections.singletonList(partitionedActivityFactory.activity(1, partitionId, i,
                new MiruActivity.Builder(tenantId, startingTimestamp + i, 0, false, new String[0]).build())));
        }

        final List<Long> timestamps = Lists.newArrayListWithCapacity(totalActivities);
        RCVSCursor cursor = new RCVSCursor(MiruPartitionedActivity.Type.ACTIVITY.getSort(), startingTimestamp, false, null);
        activityWALReader.stream(tenantId, partitionId, cursor, batchSize, -1L,
            (collisionId, partitionedActivity, timestamp) -> {
                timestamps.add(collisionId);
                return true;
            });

        assertEquals(timestamps.size(), totalActivities);
        assertEquals(timestamps.get(0).longValue(), startingTimestamp);
        assertEquals(timestamps.get(timestamps.size() - 1).longValue(), startingTimestamp + totalActivities - 1);
    }

    @Test
    public void testStreamSip() throws Exception {
        final MiruTenantId tenantId = new MiruTenantId("test".getBytes());
        final MiruPartitionId partitionId = MiruPartitionId.of(0);
        final int batchSize = 10;
        final int totalActivities = 100;
        final long startingTimestamp = 1000;

        RowColumnValueStore<MiruTenantId, MiruActivityWALRow, MiruActivityWALColumnKey, MiruPartitionedActivity, ? extends Exception> activityWAL =
            new InMemoryRowColumnValueStore<>();
        RowColumnValueStore<MiruTenantId, MiruActivityWALRow, MiruActivitySipWALColumnKey, MiruPartitionedActivity, ? extends Exception> activitySipWAL =
            new InMemoryRowColumnValueStore<>();

        RCVSActivityWALWriter activityWALWriter = new RCVSActivityWALWriter(activityWAL, activitySipWAL);
        RCVSActivityWALReader activityWALReader = new RCVSActivityWALReader(host -> 10_000, activityWAL, activitySipWAL);
        final AtomicLong clockTimestamp = new AtomicLong();
        MiruPartitionedActivityFactory partitionedActivityFactory = new MiruPartitionedActivityFactory(clockTimestamp::get);

        for (int i = 0; i < totalActivities; i++) {
            clockTimestamp.set(startingTimestamp + i);
            activityWALWriter.write(tenantId, partitionId, Collections.singletonList(partitionedActivityFactory.activity(1, partitionId, i,
                new MiruActivity.Builder(tenantId, startingTimestamp + i, 0, false, new String[0]).build())));
        }

        final List<Long> timestamps = Lists.newArrayListWithCapacity(totalActivities);
        activityWALReader.streamSip(tenantId, partitionId, RCVSSipCursor.INITIAL, null, batchSize,
            (collisionId, partitionedActivity, timestamp) -> {
                timestamps.add(collisionId);
                return true;
            },
            null);

        assertEquals(timestamps.size(), totalActivities);
        assertEquals(timestamps.get(0).longValue(), startingTimestamp);
        assertEquals(timestamps.get(timestamps.size() - 1).longValue(), startingTimestamp + totalActivities - 1);
    }

    @Test
    public void testStreamSameClock() throws Exception {
        final MiruTenantId tenantId = new MiruTenantId("test".getBytes());
        final MiruPartitionId partitionId = MiruPartitionId.of(0);
        final int batchSize = 10;
        final int totalActivities = 1_000;
        final long startingTimestamp = 1_000;

        RowColumnValueStore<MiruTenantId, MiruActivityWALRow, MiruActivityWALColumnKey, MiruPartitionedActivity, ? extends Exception> activityWAL =
            new InMemoryRowColumnValueStore<>();
        RowColumnValueStore<MiruTenantId, MiruActivityWALRow, MiruActivitySipWALColumnKey, MiruPartitionedActivity, ? extends Exception> activitySipWAL =
            new InMemoryRowColumnValueStore<>();

        RCVSActivityWALWriter activityWALWriter = new RCVSActivityWALWriter(activityWAL, activitySipWAL);
        RCVSActivityWALReader activityWALReader = new RCVSActivityWALReader(host -> 10_000, activityWAL, activitySipWAL);
        final AtomicLong clockTimestamp = new AtomicLong();
        MiruPartitionedActivityFactory partitionedActivityFactory = new MiruPartitionedActivityFactory(clockTimestamp::get);

        clockTimestamp.set(startingTimestamp);
        for (int i = 0; i < totalActivities; i++) {
            activityWALWriter.write(tenantId, partitionId, Collections.singletonList(partitionedActivityFactory.activity(1, partitionId, i,
                new MiruActivity.Builder(tenantId, startingTimestamp + i, 0, false, new String[0]).build())));
        }

        final AtomicLong expectedTimestamp = new AtomicLong(startingTimestamp);
        activityWALReader.streamSip(tenantId, partitionId, RCVSSipCursor.INITIAL, null, batchSize,
            (collisionId, partitionedActivity, timestamp) -> {
                assertEquals(partitionedActivity.clockTimestamp, startingTimestamp);
                assertEquals(partitionedActivity.timestamp, expectedTimestamp.getAndIncrement());
                return true;
            },
            null);

        assertEquals(expectedTimestamp.get(), startingTimestamp + totalActivities);
    }

}
