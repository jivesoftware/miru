package com.jivesoftware.os.miru.wal.partition;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.marshall.MiruVoidByte;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruActivitySipWALColumnKey;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruActivityWALRow;
import com.jivesoftware.os.rcvs.api.RowColumnValueStore;
import com.jivesoftware.os.rcvs.api.timestamper.ConstantTimestamper;
import com.jivesoftware.os.rcvs.api.timestamper.Timestamper;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class MiruRCVSPartitionIdProvider implements MiruPartitionIdProvider {

    private final RowColumnValueStore<MiruVoidByte, MiruTenantId, Integer, MiruPartitionId, ? extends Exception> writerPartitionRegistry;
    private final RowColumnValueStore<MiruTenantId, MiruActivityWALRow, MiruActivitySipWALColumnKey, MiruPartitionedActivity, ? extends Exception>
        activitySipWAL;

    private final ConcurrentHashMap<TenantPartitionWriterKey, AtomicInteger> cursors = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<TenantWriterKey, MiruPartitionId> tenantWriterLargestPartition = new ConcurrentHashMap<>();
    private final Cache<MiruTenantId, MiruPartitionId> tenantLargestPartition = CacheBuilder.<MiruTenantId, MiruPartitionId>newBuilder()
        .expireAfterWrite(1, TimeUnit.MINUTES)
        .build();
    private final int perClientCapacity;

    public MiruRCVSPartitionIdProvider(int totalCapacity,
        RowColumnValueStore<MiruVoidByte, MiruTenantId, Integer, MiruPartitionId, ? extends Exception> writerPartitionRegistry,
        RowColumnValueStore<MiruTenantId, MiruActivityWALRow, MiruActivitySipWALColumnKey, MiruPartitionedActivity, ? extends Exception> activitySipWAL) {
        this.writerPartitionRegistry = writerPartitionRegistry;
        this.activitySipWAL = activitySipWAL;
        this.perClientCapacity = totalCapacity;
    }

    @Override
    public MiruPartitionCursor getCursor(MiruTenantId tenantId, int writerId) throws Exception {
        MiruPartitionId partitionId = getTenantWriterLargestPartition(tenantId, writerId);
        return getCursor(tenantId, partitionId, writerId);
    }

    @Override
    public MiruPartitionCursor nextCursor(MiruTenantId tenantId, MiruPartitionCursor lastCursor, int writerId) throws Exception {
        MiruPartitionId nextPartitionId = lastCursor.getPartitionId().next();
        return getCursor(tenantId, nextPartitionId, writerId);
    }

    private MiruPartitionCursor getCursor(MiruTenantId tenantId, MiruPartitionId partitionId, int writerId) throws Exception {
        AtomicInteger index = getIndex(tenantId, partitionId, writerId);
        if (perClientCapacity < 1) {
            throw new RuntimeException("perClientCapacity is invalid. ");
        }
        return new MiruPartitionCursor(partitionId, index, perClientCapacity);
    }

    @Override
    public int getLatestIndex(MiruTenantId tenantId, MiruPartitionId partitionId, int writerId) throws Exception {
        AtomicInteger index = getIndex(tenantId, partitionId, writerId);
        return index.get();
    }

    @Override
    public void setLargestPartitionIdForWriter(MiruTenantId tenantId, MiruPartitionId partition, int writerId) throws Exception {
        // Invalid caches
        TenantWriterKey tenantWriterKey = new TenantWriterKey(tenantId, writerId);
        tenantWriterLargestPartition.remove(tenantWriterKey);
        tenantLargestPartition.invalidate(tenantId);

        Timestamper timestamper = new ConstantTimestamper(partition.getId());
        writerPartitionRegistry.add(MiruVoidByte.INSTANCE, tenantId, writerId, partition, null, timestamper);
    }

    @Override
    public MiruPartitionId getLargestPartitionIdAcrossAllWriters(final MiruTenantId tenantId) throws Exception {
        return tenantLargestPartition.get(tenantId, new Callable<MiruPartitionId>() {
            @Override
            public MiruPartitionId call() throws Exception {
                final AtomicReference<MiruPartitionId> largestPartitionId = new AtomicReference<>(MiruPartitionId.of(0));

                writerPartitionRegistry.getValues(MiruVoidByte.INSTANCE, tenantId, null, 100L, 100, false, null, null, new CallbackStream<MiruPartitionId>() {
                    @Override
                    public MiruPartitionId callback(MiruPartitionId partitionId) throws Exception {
                        if (partitionId == null) {
                            return partitionId;
                        }

                        if (partitionId.compareTo(largestPartitionId.get()) > 0) {
                            largestPartitionId.set(partitionId);
                        }

                        return partitionId;
                    }
                });

                return largestPartitionId.get();
            }
        });
    }

    private AtomicInteger getIndex(MiruTenantId tenantId, MiruPartitionId partitionId, int writerId) throws Exception {
        TenantPartitionWriterKey key = new TenantPartitionWriterKey(tenantId, partitionId, writerId);
        AtomicInteger index = cursors.get(key);
        if (index == null) {
            MiruPartitionedActivity begin = activitySipWAL.get(tenantId,
                new MiruActivityWALRow(partitionId.getId()),
                new MiruActivitySipWALColumnKey(MiruPartitionedActivity.Type.BEGIN.getSort(), (long) writerId, Long.MAX_VALUE),
                null, null);
            int lastIndex = (begin != null) ? begin.index : -1;
            cursors.putIfAbsent(key, new AtomicInteger(lastIndex));
            index = cursors.get(key);
        }
        return index;
    }

    private MiruPartitionId getTenantWriterLargestPartition(MiruTenantId tenantId, int writerId) throws Exception {
        TenantWriterKey tenantWriterKey = new TenantWriterKey(tenantId, writerId);
        MiruPartitionId tenantWriterLargestPartitionId = tenantWriterLargestPartition.get(tenantWriterKey);
        if (tenantWriterLargestPartitionId == null) {
            tenantWriterLargestPartitionId = MiruPartitionId.of(0);

            MiruPartitionId largestTenantWriterPartitionId = writerPartitionRegistry.get(MiruVoidByte.INSTANCE, tenantId, writerId, null, null);
            if (largestTenantWriterPartitionId != null) {
                tenantWriterLargestPartitionId = largestTenantWriterPartitionId;
            }

            tenantWriterLargestPartition.putIfAbsent(tenantWriterKey, tenantWriterLargestPartitionId);
        }

        return tenantWriterLargestPartition.get(tenantWriterKey);
    }

    private static class TenantPartitionWriterKey {

        private final MiruTenantId tenantId;
        private final MiruPartitionId partitionId;
        private final int writerId;

        private TenantPartitionWriterKey(MiruTenantId tenantId, MiruPartitionId partitionId, int writerId) {
            this.tenantId = tenantId;
            this.partitionId = partitionId;
            this.writerId = writerId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            TenantPartitionWriterKey that = (TenantPartitionWriterKey) o;

            if (writerId != that.writerId) {
                return false;
            }
            if (partitionId != null ? !partitionId.equals(that.partitionId) : that.partitionId != null) {
                return false;
            }
            return !(tenantId != null ? !tenantId.equals(that.tenantId) : that.tenantId != null);
        }

        @Override
        public int hashCode() {
            int result = tenantId != null ? tenantId.hashCode() : 0;
            result = 31 * result + (partitionId != null ? partitionId.hashCode() : 0);
            result = 31 * result + writerId;
            return result;
        }
    }

    private static class TenantPartitionKey {

        private final MiruTenantId tenantId;
        private final MiruPartitionId partitionId;

        private TenantPartitionKey(MiruTenantId tenantId, MiruPartitionId partitionId) {
            this.tenantId = tenantId;
            this.partitionId = partitionId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            TenantPartitionKey that = (TenantPartitionKey) o;

            if (partitionId != null ? !partitionId.equals(that.partitionId) : that.partitionId != null) {
                return false;
            }
            return !(tenantId != null ? !tenantId.equals(that.tenantId) : that.tenantId != null);
        }

        @Override
        public int hashCode() {
            int result = tenantId != null ? tenantId.hashCode() : 0;
            result = 31 * result + (partitionId != null ? partitionId.hashCode() : 0);
            return result;
        }
    }

    private static class TenantWriterKey {

        private final MiruTenantId tenantId;
        private final int writerId;

        private TenantWriterKey(MiruTenantId tenantId, int writerId) {
            this.tenantId = tenantId;
            this.writerId = writerId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            TenantWriterKey that = (TenantWriterKey) o;

            if (writerId != that.writerId) {
                return false;
            }
            return !(tenantId != null ? !tenantId.equals(that.tenantId) : that.tenantId != null);
        }

        @Override
        public int hashCode() {
            int result = tenantId != null ? tenantId.hashCode() : 0;
            result = 31 * result + writerId;
            return result;
        }
    }
}
