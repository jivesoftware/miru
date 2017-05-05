package com.jivesoftware.os.miru.api.wal;

import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.MiruReadEvent;
import com.jivesoftware.os.miru.api.activity.TimeAndVersion;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.routing.bird.shared.HostPort;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import org.apache.commons.lang.mutable.MutableLong;

import static javafx.scene.input.KeyCode.T;

/**
 * @author jonathan.colt
 */
public interface MiruWALClient<C extends MiruCursor<C, S>, S extends MiruSipCursor<S>> {

    enum RoutingGroupType {
        activity,
        readTracking
    }

    List<MiruTenantId> getAllTenantIds() throws Exception;

    void writeActivity(MiruTenantId tenantId, MiruPartitionId partitionId, List<MiruPartitionedActivity> partitionedActivities) throws Exception;

    void writeReadTracking(MiruTenantId tenantId,
        List<MiruReadEvent> readEvents,
        Function<MiruReadEvent, MiruPartitionedActivity> transformer) throws Exception;

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

    long getActivityCount(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception;

    StreamBatch<MiruWALEntry, C> getActivity(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        C cursor,
        int batchSize,
        long stopAtTimestamp,
        MutableLong bytesCount) throws Exception;

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

    OldestReadResult<S> oldestReadEventId(MiruTenantId tenantId,
        MiruStreamId streamId,
        S cursor,
        boolean createIfAbsent) throws Exception;

    StreamBatch<MiruWALEntry, Long> scanRead(MiruTenantId tenantId,
        MiruStreamId streamId,
        long fromTimestamp,
        int batchSize,
        boolean createIfAbsent) throws Exception;

    class OldestReadResult<C> {

        public long oldestEventId; // non final for json ser-der
        public C cursor; // non final for json ser-der
        public boolean endOfWAL; // non final for json ser-der

        public OldestReadResult() {
        }

        public OldestReadResult(long oldestEventId, C cursor, boolean endOfWAL) {
            this.oldestEventId = oldestEventId;
            this.cursor = cursor;
            this.endOfWAL = endOfWAL;
        }

        @Override
        public String toString() {
            return "OldestReadResult{" +
                "oldestEventId=" + oldestEventId +
                ", cursor=" + cursor +
                ", endOfWAL=" + endOfWAL +
                '}';
        }
    }

}
