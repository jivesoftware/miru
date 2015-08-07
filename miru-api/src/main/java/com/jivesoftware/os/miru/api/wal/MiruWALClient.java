package com.jivesoftware.os.miru.api.wal;

import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.routing.bird.shared.HostPort;
import java.util.List;

/**
 * @author jonathan.colt
 */
public interface MiruWALClient<C extends MiruCursor<C, S>, S extends MiruSipCursor<S>> {

    HostPort[] getTenantRoutingGroup(RoutingGroupType routingGroupType, MiruTenantId tenantId) throws Exception;

    HostPort[] getTenantPartitionRoutingGroup(RoutingGroupType routingGroupType, MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception;

    HostPort[] getTenantStreamRoutingGroup(RoutingGroupType routingGroupType, MiruTenantId tenantId, MiruStreamId streamId) throws Exception;

    enum RoutingGroupType {
        activity,
        lookup,
        readTracking
    }

    List<MiruTenantId> getAllTenantIds() throws Exception;

    void writeActivity(MiruTenantId tenantId, MiruPartitionId partitionId, List<MiruPartitionedActivity> partitionedActivities) throws Exception;

    void writeLookup(MiruTenantId tenantId, List<MiruVersionedActivityLookupEntry> entries) throws Exception;

    void writeReadTracking(MiruTenantId tenantId, MiruStreamId streamId, List<MiruPartitionedActivity> partitionedActivities) throws Exception;

    MiruPartitionId getLargestPartitionId(MiruTenantId tenantId) throws Exception;

    WriterCursor getCursorForWriterId(MiruTenantId tenantId, MiruPartitionId partitionId, int writerId) throws Exception;

    class WriterCursor {

        public int partitionId;
        public int index;

        public WriterCursor() {
        }

        public WriterCursor(int partitionId, int index) {
            this.partitionId = partitionId;
            this.index = index;
        }
    }

    MiruActivityWALStatus getActivityWALStatusForTenant(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception;

    long oldestActivityClockTimestamp(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception;

    List<MiruVersionedActivityLookupEntry> getVersionedEntries(MiruTenantId tenantId, Long[] timestamps) throws Exception;

    List<MiruLookupEntry> lookupActivity(MiruTenantId tenantId, long afterTimestamp, int batchSize) throws Exception;

    class MiruLookupEntry {

        public long collisionId;
        public long version;
        public MiruActivityLookupEntry entry;

        public MiruLookupEntry() {
        }

        public MiruLookupEntry(long collisionId, long version, MiruActivityLookupEntry entry) {
            this.collisionId = collisionId;
            this.version = version;
            this.entry = entry;
        }

        @Override
        public String toString() {
            return "MiruLookupEntry{" + "collisionId=" + collisionId + ", version=" + version + ", entry=" + entry + '}';
        }

    }

    StreamBatch<MiruWALEntry, C> getActivity(MiruTenantId tenantId,
        MiruPartitionId partitionId, C cursor, int batchSize) throws Exception;

    StreamBatch<MiruWALEntry, S> sipActivity(MiruTenantId tenantId,
        MiruPartitionId partitionId, S cursor, int batchSize) throws Exception;

    class StreamBatch<T, C> {

        public List<T> batch; // non final for json ser-der
        public C cursor; // non final for json ser-der

        public StreamBatch() {
        }

        public StreamBatch(List<T> batch, C cursor) {
            this.batch = batch;
            this.cursor = cursor;
        }

        @Override
        public String toString() {
            return "StreamBatch{" + "batch=" + batch + ", cursor=" + cursor + '}';
        }

    }

    StreamBatch<MiruWALEntry, S> getRead(MiruTenantId tenantId, MiruStreamId streamId, S cursor, long oldestEventId, int batchSize) throws Exception;

}
