package com.jivesoftware.os.miru.wal.activity;

import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruActivitySipWALColumnKey;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruActivityWALColumnKey;
import java.util.Collection;

/** @author jonathan */
public interface MiruActivityWALReader {

    interface StreamMiruActivityWAL {

        boolean stream(long collisionId, MiruPartitionedActivity partitionedActivity, long timestamp) throws Exception;
    }

    void stream(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        long afterTimestamp,
        int batchSize,
        long sleepOnFailureMillis,
        StreamMiruActivityWAL streamMiruActivityWAL)
        throws Exception;

    void streamSip(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        Sip afterSip,
        int batchSize,
        long sleepOnFailureMillis,
        StreamMiruActivityWAL streamMiruActivityWAL)
        throws Exception;

    MiruActivityWALStatus getStatus(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception;

    long countSip(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception;

    MiruPartitionedActivity findExisting(MiruTenantId tenantId, MiruPartitionId partitionId, MiruPartitionedActivity activity) throws Exception;

    long oldestActivityClockTimestamp(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception;

    void delete(MiruTenantId tenantId, MiruPartitionId partitionId, Collection<MiruActivityWALColumnKey> keys) throws Exception;

    void deleteSip(MiruTenantId tenantId, MiruPartitionId partitionId, Collection<MiruActivitySipWALColumnKey> keys) throws Exception;

    public static class Sip implements Comparable<Sip> {

        public static final Sip INITIAL = new Sip(0, 0);

        public final long clockTimestamp;
        public final long activityTimestamp;

        public Sip(long clockTimestamp, long activityTimestamp) {
            this.clockTimestamp = clockTimestamp;
            this.activityTimestamp = activityTimestamp;
        }

        @Override
        public int compareTo(Sip o) {
            int c = Long.compare(clockTimestamp, o.clockTimestamp);
            if (c == 0) {
                c = Long.compare(activityTimestamp, o.activityTimestamp);
            }
            return c;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Sip sip = (Sip) o;

            if (activityTimestamp != sip.activityTimestamp) {
                return false;
            }
            if (clockTimestamp != sip.clockTimestamp) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            int result = (int) (clockTimestamp ^ (clockTimestamp >>> 32));
            result = 31 * result + (int) (activityTimestamp ^ (activityTimestamp >>> 32));
            return result;
        }
    }
}
