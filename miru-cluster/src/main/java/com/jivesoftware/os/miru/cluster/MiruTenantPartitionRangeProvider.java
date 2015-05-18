package com.jivesoftware.os.miru.cluster;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.TenantAndPartition;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
public class MiruTenantPartitionRangeProvider {

    private final MiruWALClient<?, ?> walClient;
    private final long minimumRangeCheckInterval;
    private final ConcurrentMap<TenantAndPartition, TimestampedLookupRange> rangeCache = Maps.newConcurrentMap();
    private final StripingLocksProvider<MiruTenantId> tenantIdLocks = new StripingLocksProvider<>(128);

    public MiruTenantPartitionRangeProvider(MiruWALClient<?, ?> walClient, long minimumRangeCheckInterval) {
        this.walClient = walClient;
        this.minimumRangeCheckInterval = minimumRangeCheckInterval;
    }

    public Optional<MiruWALClient.MiruLookupRange> getRange(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        long activeTimestamp) throws Exception {

        synchronized (tenantIdLocks.lock(tenantId, 0)) {
            TenantAndPartition key = new TenantAndPartition(tenantId, partitionId);
            TimestampedLookupRange timestampedLookupRange = rangeCache.get(key);
            if (timestampedLookupRange == null || activeTimestamp > (timestampedLookupRange.timestamp + minimumRangeCheckInterval)) {
                long timestamp = System.currentTimeMillis();
                MiruWALClient.MiruLookupRange lookupRange = walClient.lookupRange(tenantId, partitionId);
                if (lookupRange == null) {
                    lookupRange = new MiruWALClient.MiruLookupRange(partitionId.getId(), -1, -1, -1, -1);
                }
                rangeCache.put(key, new TimestampedLookupRange(timestamp, lookupRange));
            }

            return timestampedLookupRange != null ? Optional.of(timestampedLookupRange.lookupRange) : Optional.<MiruWALClient.MiruLookupRange>absent();
        }
    }

    private static class TimestampedLookupRange {
        private final long timestamp;
        private final MiruWALClient.MiruLookupRange lookupRange;

        public TimestampedLookupRange(long timestamp, MiruWALClient.MiruLookupRange lookupRange) {
            this.timestamp = timestamp;
            this.lookupRange = lookupRange;
        }
    }
}
