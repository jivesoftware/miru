package com.jivesoftware.os.miru.cluster;

import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class MiruTenantPartitionRangeProvider {

    private final MiruWALClient walClient;
    private final Cache<MiruTenantId, Map<MiruPartitionId, MiruWALClient.MiruLookupRange>> rangeCache = CacheBuilder.newBuilder()
        .expireAfterWrite(1, TimeUnit.DAYS) //TODO expose to config
        .build();

    public MiruTenantPartitionRangeProvider(MiruWALClient walClient) {
        this.walClient = walClient;
    }

    public Optional<MiruWALClient.MiruLookupRange> getRange(final MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        Map<MiruPartitionId, MiruWALClient.MiruLookupRange> partitionLookupRange = rangeCache.get(tenantId,
            new Callable<Map<MiruPartitionId, MiruWALClient.MiruLookupRange>>() {
                @Override
                public Map<MiruPartitionId, MiruWALClient.MiruLookupRange> call() throws Exception {
                    Collection<MiruWALClient.MiruLookupRange> ranges = walClient.lookupRanges(tenantId);
                    Map<MiruPartitionId, MiruWALClient.MiruLookupRange> partitionLookupRange = Maps.newConcurrentMap();
                    if (ranges != null) {
                        for (MiruWALClient.MiruLookupRange range : ranges) {
                            partitionLookupRange.put(MiruPartitionId.of(range.partitionId), range);
                        }
                    }
                    return partitionLookupRange;
                }
            });
        return Optional.fromNullable(partitionLookupRange.get(partitionId));
    }
}
