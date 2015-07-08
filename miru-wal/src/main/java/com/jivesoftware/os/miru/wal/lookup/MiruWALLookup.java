package com.jivesoftware.os.miru.wal.lookup;

import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.wal.MiruActivityLookupEntry;
import java.util.List;

/**
 *
 */
public interface MiruWALLookup {

    List<MiruTenantId> allTenantIds() throws Exception;

    MiruVersionedActivityLookupEntry[] getVersionedEntries(MiruTenantId tenantId, Long[] activityTimestamp) throws Exception;

    void add(MiruTenantId tenantId, List<MiruPartitionedActivity> activities) throws Exception;

    void stream(MiruTenantId tenantId, long afterTimestamp, StreamLookupEntry streamLookupEntry) throws Exception;

    void streamRanges(MiruTenantId tenantId, MiruPartitionId partitionId, StreamRangeLookup streamRangeLookup) throws Exception;

    void putRange(MiruTenantId tenantId, MiruPartitionId partitionId, RangeMinMax minMax) throws Exception;

    void removeRange(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception;

    interface StreamLookupEntry {

        boolean stream(long activityTimestamp, MiruActivityLookupEntry entry, long version) throws Exception;
    }

    interface StreamRangeLookup {

        boolean stream(MiruPartitionId partitionId, RangeType type, long timestamp, long version);
    }

    enum RangeType {

        orderIdMin((byte) 0),
        orderIdMax((byte) 1),
        clockMin((byte) 2),
        clockMax((byte) 3);

        private final byte type;

        RangeType(byte type) {
            this.type = type;
        }

        public byte getType() {
            return type;
        }

        public static RangeType fromType(byte type) {
            for (RangeType rangeType : values()) {
                if (rangeType.type == type) {
                    return rangeType;
                }
            }
            return null;
        }
    }
}
