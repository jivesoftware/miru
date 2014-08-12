package com.jivesoftware.os.miru.client.memory;

import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.client.MiruClientConfig;
import com.jivesoftware.os.miru.client.MiruPartitionCursor;
import com.jivesoftware.os.miru.client.MiruPartitionIdProvider;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.inject.Inject;
import javax.inject.Singleton;

/** @author jonathan */
@Singleton
public class MiruInMemoryPartitionIdProvider implements MiruPartitionIdProvider {

    private final ConcurrentHashMap<TenantPartitionWriterKey, AtomicInteger> cursors = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<TenantWriterKey, MiruPartitionId> tenantWriterLargestPartition = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<MiruTenantId, MiruPartitionId> tenantLargestPartition = new ConcurrentHashMap<>();
    private final int totalCapacity;
    private int perClientCapacity;

    @Inject
    public MiruInMemoryPartitionIdProvider(MiruClientConfig config) {
        this.totalCapacity = config.getTotalCapacity();
        // TODO - wire up a task that periodically checks how many writers we have and divide totalCapacity by that number
        this.perClientCapacity = this.totalCapacity;
    }

    @Override
    public MiruPartitionCursor getCursor(MiruTenantId tenantId, int writerId) {
        MiruPartitionId partitionId = getTenantWriterLargestPartition(tenantId, writerId);
        return getCursor(tenantId, partitionId, writerId);
    }

    @Override
    public MiruPartitionCursor nextCursor(MiruTenantId tenantId, MiruPartitionCursor lastCursor, int writerId) {
        MiruPartitionId nextPartitionId = lastCursor.getPartitionId().next();
        return getCursor(tenantId, nextPartitionId, writerId);
    }

    private MiruPartitionCursor getCursor(MiruTenantId tenantId, MiruPartitionId partitionId, int writerId) {
        AtomicInteger index = getIndex(tenantId, partitionId, writerId);
        return new MiruPartitionCursor(partitionId, index, perClientCapacity);
    }

    @Override
    public int getLatestIndex(MiruTenantId tenantId, MiruPartitionId partitionId, int writerId) {
        AtomicInteger index = getIndex(tenantId, partitionId, writerId);
        return index.get();
    }

    @Override
    public void setLargestPartitionIdForWriter(MiruTenantId tenantId, MiruPartitionId partition, int writerId) {
        tenantWriterLargestPartition.put(new TenantWriterKey(tenantId, writerId), partition);

        MiruPartitionId got = tenantLargestPartition.get(tenantId);
        while (got.compareTo(partition) < 0) {
            if (tenantLargestPartition.replace(tenantId, got, partition)) {
                break;
            }
            got = tenantLargestPartition.get(tenantId);
        }
    }

    @Override
    public MiruPartitionId getLargestPartitionIdAcrossAllWriters(MiruTenantId tenantId) {
        return tenantLargestPartition.get(tenantId);
    }

    @Override
    public long minTimeForPartition(MiruTenantId tenantId, MiruPartitionId partitionId) {
        return 0;
    }

    private AtomicInteger getIndex(MiruTenantId tenantId, MiruPartitionId partitionId, int writerId) {
        TenantPartitionWriterKey key = new TenantPartitionWriterKey(tenantId, partitionId, writerId);
        AtomicInteger index = cursors.get(key);
        if (index == null) {
            cursors.putIfAbsent(key, new AtomicInteger(-1));
            index = cursors.get(key);
        }
        return index;
    }

    private MiruPartitionId getTenantWriterLargestPartition(MiruTenantId tenantId, int writerId) {
        TenantWriterKey key = new TenantWriterKey(tenantId, writerId);
        MiruPartitionId partitionId = tenantWriterLargestPartition.get(key);
        if (partitionId == null) {
            tenantWriterLargestPartition.putIfAbsent(key, MiruPartitionId.of(0));
            partitionId = tenantWriterLargestPartition.get(key);
        }
        return partitionId;
    }

    static class TenantPartitionWriterKey {

        final MiruTenantId tenantId;
        final MiruPartitionId partitionId;
        final int writerId;

        TenantPartitionWriterKey(MiruTenantId tenantId, MiruPartitionId partitionId, int writerId) {
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
            if (tenantId != null ? !tenantId.equals(that.tenantId) : that.tenantId != null) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            int result = tenantId != null ? tenantId.hashCode() : 0;
            result = 31 * result + (partitionId != null ? partitionId.hashCode() : 0);
            result = 31 * result + writerId;
            return result;
        }
    }

    static class PartitionAndIndex {

        final MiruPartitionId partition;
        final int index;

        public PartitionAndIndex(MiruPartitionId partition, int index) {
            this.partition = partition;
            this.index = index;
        }

    }

    static class TenantWriterKey {

        private final MiruTenantId tenantId;
        private final int writerId;

        public TenantWriterKey(MiruTenantId tenantId, int writerId) {
            this.tenantId = tenantId;
            this.writerId = writerId;
        }

        @Override
        public int hashCode() {
            int hash = 5;
            hash = 97 * hash + Objects.hashCode(this.tenantId);
            hash = 97 * hash + this.writerId;
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final TenantWriterKey other = (TenantWriterKey) obj;
            if (!Objects.equals(this.tenantId, other.tenantId)) {
                return false;
            }
            if (this.writerId != other.writerId) {
                return false;
            }
            return true;
        }
    }
}
