package com.jivesoftware.os.miru.api.wal;

import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import java.util.Collection;
import java.util.List;

/**
 *
 * @author jonathan.colt
 */
public interface MiruWALClient {

    List<MiruTenantId> getAllTenantIds() throws Exception;

    MiruPartitionId getLargestPartitionIdAcrossAllWriters(MiruTenantId tenantId) throws Exception;

    List<MiruActivityWALStatus> getPartitionStatus(MiruTenantId tenantId, List<MiruPartitionId> partitionIds) throws Exception;

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

    Collection<MiruLookupRange> lookupRanges(MiruTenantId tenantId) throws Exception;

    class MiruLookupRange {

        public int partitionId;
        public long minClock;
        public long maxClock;
        public long minOrderId;
        public long maxOrderId;

        public MiruLookupRange() {
        }

        public MiruLookupRange(int partitionId, long minClock, long maxClock, long minOrderId, long maxOrderId) {
            this.partitionId = partitionId;
            this.minClock = minClock;
            this.maxClock = maxClock;
            this.minOrderId = minOrderId;
            this.maxOrderId = maxOrderId;
        }
    }

    StreamBatch<MiruWALEntry, SipActivityCursor> sipActivity(MiruTenantId tenantId,
        MiruPartitionId partitionId, SipActivityCursor cursor, int batchSize) throws Exception;

    StreamBatch<MiruWALEntry, GetActivityCursor> getActivity(MiruTenantId tenantId,
        MiruPartitionId partitionId, GetActivityCursor cursor, int batchSize) throws Exception;

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

    class SipActivityCursor {

        public byte sort; // non final for json ser-der
        public long collisionId; // non final for json ser-der
        public long sipId; // non final for json ser-der

        public SipActivityCursor() {
        }

        public SipActivityCursor(byte sort, long collisionId, long sipId) {
            this.sort = sort;
            this.collisionId = collisionId;
            this.sipId = sipId;
        }

        @Override
        public String toString() {
            return "SipActivityCursor{" + "sort=" + sort + ", collisionId=" + collisionId + ", sipId=" + sipId + '}';
        }

    }

    class GetActivityCursor {

        public byte sort; // non final for json ser-der
        public long collisionId; // non final for json ser-der

        public GetActivityCursor() {
        }

        public GetActivityCursor(byte sort, long collisionId) {
            this.sort = sort;
            this.collisionId = collisionId;
        }

        @Override
        public String toString() {
            return "GetActivityCursor{" + "sort=" + sort + ", collisionId=" + collisionId + '}';
        }

    }

    StreamBatch<MiruReadSipEntry, SipReadCursor> sipRead(MiruTenantId tenantId,
        MiruStreamId streamId, SipReadCursor cursor, int batchSize) throws Exception;

    StreamBatch<MiruWALEntry, GetReadCursor> getRead(MiruTenantId tenantId,
        MiruStreamId streamId, GetReadCursor cursor, int batchSize) throws Exception;

    class SipReadCursor {

        public long sipId; // non final for json ser-der
        public long eventId; // non final for json ser-der

        public SipReadCursor() {
        }

        public SipReadCursor(long sipId, long eventId) {
            this.sipId = sipId;
            this.eventId = eventId;
        }

        @Override
        public String toString() {
            return "SipReadCursor{" + "sipId=" + sipId + ", eventId=" + eventId + '}';
        }

    }

    class GetReadCursor {

        public long eventId; // non final for json ser-der

        public GetReadCursor() {
        }

        public GetReadCursor(long eventId) {
            this.eventId = eventId;
        }

        @Override
        public String toString() {
            return "GetReadCursor{" + "eventId=" + eventId + '}';
        }

    }
}
