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
public class MiruDeltaAuthzIndex<BM extends IBM, IBM> implements MiruAuthzIndex<IBM>, Mergeable {

    private final MiruBitmaps<BM, IBM> bitmaps;
    private final long indexId;
    private final MiruAuthzCache<BM, IBM> cache;
    private final MiruAuthzIndex<IBM> backingIndex;
    private final Cache<MiruFieldIndex.IndexKey, Optional<?>> fieldIndexCache;
    private final ConcurrentMap<String, MiruDeltaInvertedIndex<BM, IBM>> authzDeltas = Maps.newConcurrentMap();

    public MiruDeltaAuthzIndex(MiruBitmaps<BM, IBM> bitmaps,
        long indexId,
        MiruAuthzCache<BM, IBM> cache,
        MiruAuthzIndex<IBM> backingIndex,
        Cache<MiruFieldIndex.IndexKey, Optional<?>> fieldIndexCache) {
        this.bitmaps = bitmaps;
        this.indexId = indexId;
        this.cache = cache;
        this.backingIndex = backingIndex;
        this.fieldIndexCache = fieldIndexCache;
    }

    @Override
    public MiruInvertedIndex<IBM> getAuthz(String authz) throws Exception {
        MiruDeltaInvertedIndex<BM, IBM> delta = authzDeltas.get(authz);
        if (delta != null) {
            return delta;
        } else {
            return backingIndex.getAuthz(authz);
        }
    }

    @Override
    public BM getCompositeAuthz(MiruAuthzExpression authzExpression, byte[] primitiveBuffer) throws Exception {
        return cache.getOrCompose(authzExpression, authz -> getAuthz(authz).getIndex(primitiveBuffer).orNull());
    }

    @Override
    public void append(String authz, byte[] primitiveBuffer, int... ids) throws Exception {
        getOrCreate(authz).append(primitiveBuffer, ids);
        cache.increment(authz);
    }

    @Override
    public void set(String authz, byte[] primitiveBuffer, int... ids) throws Exception {
        getOrCreate(authz).set(primitiveBuffer, ids);
        cache.increment(authz);
    }

    @Override
    public void remove(String authz, int id, byte[] primitiveBuffer) throws Exception {
        getOrCreate(authz).remove(id, primitiveBuffer);
        cache.increment(authz);
    }

    private MiruDeltaInvertedIndex<BM, IBM> getOrCreate(String authz) throws Exception {
        MiruDeltaInvertedIndex<BM, IBM> delta = authzDeltas.get(authz);
        if (delta == null) {
            delta = new MiruDeltaInvertedIndex<>(bitmaps, backingIndex.getAuthz(authz), new MiruDeltaInvertedIndex.Delta<IBM>(),
                new MiruFieldIndex.IndexKey(indexId, MiruAuthzUtils.key(authz)), fieldIndexCache, null);
            MiruDeltaInvertedIndex<BM, IBM> existing = authzDeltas.putIfAbsent(authz, delta);
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
    public void merge(byte[] primitiveBuffer) throws Exception {
        for (Map.Entry<String, MiruDeltaInvertedIndex<BM, IBM>> entry : authzDeltas.entrySet()) {
            entry.getValue().merge(primitiveBuffer);
            cache.increment(entry.getKey());
        }
        authzDeltas.clear();
    }
}
