package com.jivesoftware.os.miru.service.index.delta;

import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruAuthzIndex;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.service.index.Mergeable;
import com.jivesoftware.os.miru.service.index.auth.MiruAuthzCache;
import com.jivesoftware.os.miru.service.index.auth.MiruAuthzUtils;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * DELTA FORCE
 */
public class MiruDeltaAuthzIndex<BM> implements MiruAuthzIndex<BM>, Mergeable {

    private final MiruBitmaps<BM> bitmaps;
    private final long indexId;
    private final MiruAuthzCache<BM> cache;
    private final MiruAuthzIndex<BM> backingIndex;
    private final Cache<MiruFieldIndex.IndexKey, Optional<?>> fieldIndexCache;
    private final ConcurrentMap<String, MiruDeltaInvertedIndex<BM>> authzDeltas = Maps.newConcurrentMap();

    public MiruDeltaAuthzIndex(MiruBitmaps<BM> bitmaps,
        long indexId,
        MiruAuthzCache<BM> cache,
        MiruAuthzIndex<BM> backingIndex,
        Cache<MiruFieldIndex.IndexKey, Optional<?>> fieldIndexCache) {
        this.bitmaps = bitmaps;
        this.indexId = indexId;
        this.cache = cache;
        this.backingIndex = backingIndex;
        this.fieldIndexCache = fieldIndexCache;
    }

    @Override
    public MiruInvertedIndex<BM> getAuthz(String authz) throws Exception {
        MiruDeltaInvertedIndex<BM> delta = authzDeltas.get(authz);
        if (delta != null) {
            return delta;
        } else {
            return backingIndex.getAuthz(authz);
        }
    }

    @Override
    public BM getCompositeAuthz(MiruAuthzExpression authzExpression) throws Exception {
        return cache.getOrCompose(authzExpression, new MiruAuthzUtils.IndexRetriever<BM>() {
            @Override
            public BM getIndex(String authz) throws Exception {
                return getAuthz(authz).getIndex().orNull();
            }
        });
    }

    @Override
    public void append(String authz, int... ids) throws Exception {
        getOrCreate(authz).append(ids);
        cache.increment(authz);
    }

    @Override
    public void set(String authz, int... ids) throws Exception {
        getOrCreate(authz).set(ids);
        cache.increment(authz);
    }

    @Override
    public void remove(String authz, int id) throws Exception {
        getOrCreate(authz).remove(id);
        cache.increment(authz);
    }

    private MiruDeltaInvertedIndex<BM> getOrCreate(String authz) throws Exception {
        MiruDeltaInvertedIndex<BM> delta = authzDeltas.get(authz);
        if (delta == null) {
            delta = new MiruDeltaInvertedIndex<>(bitmaps, backingIndex.getAuthz(authz), new MiruDeltaInvertedIndex.Delta<BM>(),
                new MiruFieldIndex.IndexKey(indexId, MiruAuthzUtils.key(authz)), fieldIndexCache);
            MiruDeltaInvertedIndex<BM> existing = authzDeltas.putIfAbsent(authz, delta);
            if (existing != null) {
                delta = existing;
            }
        }
        return delta;
    }

    @Override
    public void close() {
        cache.clear();
        backingIndex.close();
    }

    @Override
    public void merge() throws Exception {
        for (Map.Entry<String, MiruDeltaInvertedIndex<BM>> entry : authzDeltas.entrySet()) {
            entry.getValue().merge();
            cache.increment(entry.getKey());
        }
        authzDeltas.clear();
    }
}
