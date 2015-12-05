package com.jivesoftware.os.miru.service.index.delta;

import com.google.common.collect.Maps;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruAuthzIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.plugin.partition.TrackError;
import com.jivesoftware.os.miru.service.index.Mergeable;
import com.jivesoftware.os.miru.service.index.auth.MiruAuthzCache;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * DELTA FORCE
 */
public class MiruDeltaAuthzIndex<BM extends IBM, IBM> implements MiruAuthzIndex<BM, IBM>, Mergeable {

    private final MiruBitmaps<BM, IBM> bitmaps;
    private final TrackError trackError;
    private final MiruAuthzCache<BM, IBM> cache;
    private final MiruAuthzIndex<BM, IBM> backingIndex;
    private final ConcurrentMap<String, MiruDeltaInvertedIndex<BM, IBM>> authzDeltas = Maps.newConcurrentMap();

    public MiruDeltaAuthzIndex(MiruBitmaps<BM, IBM> bitmaps,
        TrackError trackError,
        MiruAuthzCache<BM, IBM> cache,
        MiruAuthzIndex<BM, IBM> backingIndex) {
        this.bitmaps = bitmaps;
        this.trackError = trackError;
        this.cache = cache;
        this.backingIndex = backingIndex;
    }

    @Override
    public MiruInvertedIndex<BM, IBM> getAuthz(String authz) throws Exception {
        MiruDeltaInvertedIndex<BM, IBM> delta = authzDeltas.get(authz);
        if (delta != null) {
            return delta;
        } else {
            return backingIndex.getAuthz(authz);
        }
    }

    @Override
    public BM getCompositeAuthz(MiruAuthzExpression authzExpression, StackBuffer stackBuffer) throws Exception {
        return cache.getOrCompose(authzExpression, authz -> getAuthz(authz).getIndex(stackBuffer).orNull());
    }

    @Override
    public void append(String authz, StackBuffer stackBuffer, int... ids) throws Exception {
        getOrCreate(authz).append(stackBuffer, ids);
        cache.increment(authz);
    }

    @Override
    public void set(String authz, StackBuffer stackBuffer, int... ids) throws Exception {
        getOrCreate(authz).set(stackBuffer, ids);
        cache.increment(authz);
    }

    @Override
    public void remove(String authz, int id, StackBuffer stackBuffer) throws Exception {
        getOrCreate(authz).remove(id, stackBuffer);
        cache.increment(authz);
    }

    private MiruDeltaInvertedIndex<BM, IBM> getOrCreate(String authz) throws Exception {
        MiruDeltaInvertedIndex<BM, IBM> delta = authzDeltas.get(authz);
        if (delta == null) {
            delta = new MiruDeltaInvertedIndex<>(bitmaps, trackError, backingIndex.getAuthz(authz), new MiruDeltaInvertedIndex.Delta<IBM>());
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
    public void merge(StackBuffer stackBuffer) throws Exception {
        for (Map.Entry<String, MiruDeltaInvertedIndex<BM, IBM>> entry : authzDeltas.entrySet()) {
            entry.getValue().merge(stackBuffer);
            cache.increment(entry.getKey());
        }
        authzDeltas.clear();
    }
}
