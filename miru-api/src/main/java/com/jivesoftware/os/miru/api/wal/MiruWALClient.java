package com.jivesoftware.os.miru.api.wal;

import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.TimeAndVersion;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.routing.bird.shared.HostPort;
import java.util.List;
import java.util.Set;

/**
 * @author jonathan.colt
 */
public interface MiruWALClient<C extends MiruCursor<C, S>, S extends MiruSipCursor<S>> {

    HostPort[] getTenantRoutingGroup(RoutingGroupType routingGroupType, MiruTenantId tenantId) throws Exception;

    HostPort[] getTenantPartitionRoutingGroup(RoutingGroupType routingGroupType, MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception;

    HostPort[] getTenantStreamRoutingGroup(RoutingGroupType routingGroupType, MiruTenantId tenantId, MiruStreamId streamId) throws Exception;

    enum RoutingGroupType {
        activity,
        readTracking
    }

    List<MiruTenantId> getAllTenantIds() throws Exception;

    void writeActivity(MiruTenantId tenantId, MiruPartitionId partitionId, List<MiruPartitionedActivity> partitionedActivities) throws Exception;

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

    List<MiruVersionedActivityLookupEntry> getVersionedEntries(MiruTenantId tenantId, MiruPartitionId partitionId, Long[] timestamps) throws Exception;

    StreamBatch<MiruWALEntry, C> getActivity(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        C cursor,
        int batchSize,
        long stopAtTimestamp) throws Exception;

    StreamBatch<MiruWALEntry, S> sipActivity(MiruTenantId tenantId,
        MiruPartitionId partitionId, S cursor, Set<TimeAndVersion> lastSeen, int batchSize) throws Exception;

    class StreamBatch<T, C> {

        public List<T> activities; // non final for json ser-der
        public C cursor; // non final for json ser-der
        public boolean endOfWAL; // non final for json ser-der
        public Set<TimeAndVersion> suppressed;

        public StreamBatch() {
        }

        public StreamBatch(List<T> activities, C cursor, boolean endOfWAL, Set<TimeAndVersion> suppressed) {
            this.activities = activities;
            this.cursor = cursor;
            this.endOfWAL = endOfWAL;
            this.suppressed = suppressed;
        }

        @Override
        public String toString() {
            return "StreamBatch{" +
                "activities=" + activities +
                ", cursor=" + cursor +
                ", endOfWAL=" + endOfWAL +
                ", suppressed=" + suppressed +
                '}';
        }
    }

    StreamBatch<MiruWALEntry, S> getRead(MiruTenantId tenantId, MiruStreamId streamId, S cursor, long oldestEventId, int batchSize) throws Exception;

}
