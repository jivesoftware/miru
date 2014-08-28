package com.jivesoftware.os.miru.service.index.memory;

import com.google.common.collect.ImmutableMap;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.query.MiruAuthzIndex;
import com.jivesoftware.os.miru.query.MiruBitmaps;
import com.jivesoftware.os.miru.query.MiruInvertedIndex;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.BulkImport;
import com.jivesoftware.os.miru.service.index.auth.MiruAuthzCache;
import com.jivesoftware.os.miru.service.index.auth.MiruAuthzUtils;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author jonathan
 */
public class MiruInMemoryAuthzIndex<BM> implements MiruAuthzIndex<BM>, BulkImport<Map<String, MiruInvertedIndex<BM>>>,
        BulkExport<Map<String, MiruInvertedIndex<BM>>> {

    private final MiruBitmaps<BM> bitmaps;
    private final ConcurrentMap<String, MiruInvertedIndex<BM>> index;
    private final MiruAuthzCache<BM> cache;

    public MiruInMemoryAuthzIndex(MiruBitmaps<BM> bitmaps, MiruAuthzCache<BM> cache) {
        this.bitmaps = bitmaps;
        this.index = new ConcurrentHashMap<>();
        this.cache = cache;
    }

    @Override
    public long sizeInMemory() throws Exception {
        long sizeInBytes = 0;
        for (MiruInvertedIndex<BM> i : index.values()) {
            sizeInBytes += bitmaps.sizeInBytes(i.getIndex());
        }
        sizeInBytes += cache.sizeInBytes();
        return sizeInBytes;
    }

    @Override
    public long sizeOnDisk() throws Exception {
        return 0;
    }

    @Override
    public void index(String authz, int id) throws Exception {
        getOrAllocate(authz, true).append(id);
        cache.increment(authz);
    }

    @Override
    public void repair(String authz, int id, boolean value) throws Exception {
        MiruInvertedIndex got = index.get(authz);
        if (got != null) {
            if (value) {
                got.set(id);
            } else {
                got.remove(id);
            }
            cache.increment(authz);
        }
    }

    @Override
    public void close() {
        cache.clear();
    }

    private MiruInvertedIndex getOrAllocate(String authz, boolean allocateIfAbsent) {
        MiruInvertedIndex<BM> got = index.get(authz);
        if (got == null && allocateIfAbsent) {
            index.putIfAbsent(authz, new MiruInMemoryInvertedIndex<>(bitmaps));
            got = index.get(authz);
        }
        return got;
    }

    @Override
    public BM getCompositeAuthz(MiruAuthzExpression authzExpression) throws Exception {
        return cache.getOrCompose(authzExpression, new MiruAuthzUtils.IndexRetriever<BM>() {
            @Override
            public BM getIndex(String authz) throws Exception {
                MiruInvertedIndex<BM> index = getOrAllocate(authz, false);
                return index != null ? index.getIndex() : null;
            }
        });
    }

    @Override
    public Map<String, MiruInvertedIndex<BM>> bulkExport(MiruTenantId tenantId) throws Exception {
        return ImmutableMap.<String, MiruInvertedIndex<BM>>copyOf(index);
    }

    @Override
    public void bulkImport(MiruTenantId tenantId, BulkExport<Map<String, MiruInvertedIndex<BM>>> importItems) throws Exception {
        this.index.putAll(importItems.bulkExport(tenantId));
    }

}
