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

    private MiruDeltaInvertedIndex<BM> getOrCreate(String authz) throws Exception {
        MiruDeltaInvertedIndex<BM> delta = authzDeltas.get(authz);
        if (delta == null) {
            delta = new MiruDeltaInvertedIndex<>(bitmaps, backingIndex.getAuthz(authz), new MiruDeltaInvertedIndex.Delta<BM>(),
                new MiruFieldIndex.IndexKey(indexId, MiruAuthzUtils.key(authz)), fieldIndexCache, null);
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
    public void merge(byte[] primitiveBuffer) throws Exception {
        for (Map.Entry<String, MiruDeltaInvertedIndex<BM>> entry : authzDeltas.entrySet()) {
            entry.getValue().merge(primitiveBuffer);
            cache.increment(entry.getKey());
        }
        authzDeltas.clear();
    }
}
