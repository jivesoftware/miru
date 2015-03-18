package com.jivesoftware.os.miru.wal.activity;

import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivityFactory;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.marshall.MiruVoidByte;
import com.jivesoftware.os.miru.api.wal.Sip;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruActivitySipWALColumnKey;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruActivityWALColumnKey;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruActivityWALRow;
import com.jivesoftware.os.rcvs.api.RowColumnValueStore;
import com.jivesoftware.os.rcvs.inmemory.InMemoryRowColumnValueStore;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class MiruActivityWALReaderImplTest {

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

        MiruActivityWALWriter activityWALWriter = new MiruWriteToActivityAndSipWAL(activityWAL, activitySipWAL);
        MiruActivityWALReader activityWALReader = new MiruActivityWALReaderImpl(activityWAL, activitySipWAL);
        MiruPartitionedActivityFactory partitionedActivityFactory = new MiruPartitionedActivityFactory();

        for (int i = 0; i < totalActivities; i++) {
            activityWALWriter.write(tenantId, Collections.singletonList(partitionedActivityFactory.activity(1, partitionId, i,
                new MiruActivity.Builder(tenantId, startingTimestamp + i, new String[0], 0).build())));
        }

        final List<Long> timestamps = Lists.newArrayListWithCapacity(totalActivities);
        activityWALReader.stream(tenantId, partitionId, MiruPartitionedActivity.Type.ACTIVITY.getSort(),
            startingTimestamp, batchSize, new MiruActivityWALReader.StreamMiruActivityWAL() {
                @Override
                public boolean stream(long collisionId, MiruPartitionedActivity partitionedActivity, long timestamp) throws Exception {
                    timestamps.add(collisionId);
                    return true;
                }
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
        RowColumnValueStore<MiruVoidByte, MiruTenantId, Integer, MiruPartitionId, ? extends Exception> writerPartitionRegistry =
            new InMemoryRowColumnValueStore<>();

        MiruActivityWALWriter activityWALWriter = new MiruWriteToActivityAndSipWAL(activityWAL, activitySipWAL);
        MiruActivityWALReader activityWALReader = new MiruActivityWALReaderImpl(activityWAL, activitySipWAL);
        final AtomicLong clockTimestamp = new AtomicLong();
        MiruPartitionedActivityFactory partitionedActivityFactory = new MiruPartitionedActivityFactory(new MiruPartitionedActivityFactory.ClockTimestamper() {
            @Override
            public long get() {
                return clockTimestamp.get();
            }
        });

        for (int i = 0; i < totalActivities; i++) {
            clockTimestamp.set(startingTimestamp + i);
            activityWALWriter.write(tenantId, Collections.singletonList(partitionedActivityFactory.activity(1, partitionId, i,
                new MiruActivity.Builder(tenantId, startingTimestamp + i, new String[0], 0).build())));
        }

        final List<Long> timestamps = Lists.newArrayListWithCapacity(totalActivities);
        activityWALReader.streamSip(tenantId, partitionId, MiruPartitionedActivity.Type.ACTIVITY.getSort(),
            Sip.INITIAL, batchSize,
            new MiruActivityWALReader.StreamMiruActivityWAL() {
                @Override
                public boolean stream(long collisionId, MiruPartitionedActivity partitionedActivity, long timestamp) throws Exception {
                    timestamps.add(collisionId);
                    return true;
                }
            });

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
        RowColumnValueStore<MiruVoidByte, MiruTenantId, Integer, MiruPartitionId, ? extends Exception> writerPartitionRegistry =
            new InMemoryRowColumnValueStore<>();

        MiruActivityWALWriter activityWALWriter = new MiruWriteToActivityAndSipWAL(activityWAL, activitySipWAL);
        MiruActivityWALReader activityWALReader = new MiruActivityWALReaderImpl(activityWAL, activitySipWAL);
        final AtomicLong clockTimestamp = new AtomicLong();
        MiruPartitionedActivityFactory partitionedActivityFactory = new MiruPartitionedActivityFactory(new MiruPartitionedActivityFactory.ClockTimestamper() {
            @Override
            public long get() {
                return clockTimestamp.get();
            }
        });

        clockTimestamp.set(startingTimestamp);
        for (int i = 0; i < totalActivities; i++) {
            activityWALWriter.write(tenantId, Collections.singletonList(partitionedActivityFactory.activity(1, partitionId, i,
                new MiruActivity.Builder(tenantId, startingTimestamp + i, new String[0], 0).build())));
        }

        final AtomicLong expectedTimestamp = new AtomicLong(startingTimestamp);
        activityWALReader.streamSip(tenantId, partitionId, MiruPartitionedActivity.Type.ACTIVITY.getSort(), Sip.INITIAL, batchSize,
            new MiruActivityWALReader.StreamMiruActivityWAL() {
                @Override
                public boolean stream(long collisionId, MiruPartitionedActivity partitionedActivity, long timestamp) throws Exception {
                    assertEquals(partitionedActivity.clockTimestamp, startingTimestamp);
                    assertEquals(partitionedActivity.timestamp, expectedTimestamp.getAndIncrement());
                    return true;
                }
            });

        assertEquals(expectedTimestamp.get(), startingTimestamp + totalActivities);
    }

}
