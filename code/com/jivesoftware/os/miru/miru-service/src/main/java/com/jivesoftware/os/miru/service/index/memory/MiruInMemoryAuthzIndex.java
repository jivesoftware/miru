package com.jivesoftware.os.miru.service.index.memory;

import com.google.common.collect.ImmutableMap;
import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.BulkImport;
import com.jivesoftware.os.miru.service.index.auth.MiruAuthzCache;
import com.jivesoftware.os.miru.service.index.auth.MiruAuthzIndex;
import com.jivesoftware.os.miru.service.index.auth.MiruAuthzUtils;
import com.jivesoftware.os.miru.service.index.MiruInvertedIndex;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** @author jonathan */
public class MiruInMemoryAuthzIndex implements MiruAuthzIndex, BulkImport<Map<String, MiruInvertedIndex>>, BulkExport<Map<String, MiruInvertedIndex>> {

    private final ConcurrentMap<String, MiruInvertedIndex> index;
    private final MiruAuthzCache cache;

    public MiruInMemoryAuthzIndex(MiruAuthzCache cache) {
        this.index = new ConcurrentHashMap<>();
        this.cache = cache;
    }

    @Override
    public long sizeInMemory() throws Exception {
        long sizeInBytes = 0;
        for (MiruInvertedIndex i : index.values()) {
            sizeInBytes += i.getIndex().sizeInBytes();
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
        MiruInvertedIndex got = index.get(authz);
        if (got == null && allocateIfAbsent) {
            index.putIfAbsent(authz, new MiruInMemoryInvertedIndex(new EWAHCompressedBitmap()));
            got = index.get(authz);
        }
        return got;
    }

    @Override
    public EWAHCompressedBitmap getCompositeAuthz(MiruAuthzExpression authzExpression) throws Exception {
        return cache.getOrCompose(authzExpression, new MiruAuthzUtils.IndexRetriever() {
            @Override
            public EWAHCompressedBitmap getIndex(String authz) throws Exception {
                MiruInvertedIndex index = getOrAllocate(authz, false);
                return index != null ? index.getIndex() : null;
            }
        });
    }

    @Override
    public Map<String, MiruInvertedIndex> bulkExport() throws Exception {
        return ImmutableMap.copyOf(index);
    }

    @Override
    public void bulkImport(BulkExport<Map<String, MiruInvertedIndex>> importItems) throws Exception {
        this.index.putAll(importItems.bulkExport());
    }

}
