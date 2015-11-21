package com.jivesoftware.os.miru.service.index.delta;

import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.service.index.Mergeable;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * DELTA FORCE
 */
public class MiruDeltaInvertedIndex<BM extends IBM, IBM> implements MiruInvertedIndex<IBM>, Mergeable {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruBitmaps<BM, IBM> bitmaps;
    private final MiruInvertedIndex<IBM> backingIndex;
    private final Delta<IBM> delta;
    private final MiruFieldIndex.IndexKey indexKey;
    private final Cache<MiruFieldIndex.IndexKey, Optional<?>> fieldIndexCache;
    private final Cache<MiruFieldIndex.IndexKey, Long> versionCache;

    public MiruDeltaInvertedIndex(MiruBitmaps<BM, IBM> bitmaps,
        MiruInvertedIndex<IBM> backingIndex,
        Delta<IBM> delta,
        MiruFieldIndex.IndexKey indexKey,
        Cache<MiruFieldIndex.IndexKey, Optional<?>> fieldIndexCache,
        Cache<MiruFieldIndex.IndexKey, Long> versionCache) {
        this.bitmaps = bitmaps;
        this.backingIndex = backingIndex;
        this.delta = delta;
        this.indexKey = indexKey;
        this.fieldIndexCache = fieldIndexCache;
        this.versionCache = versionCache;
    }

    private final Callable<Optional<IBM>> indexLoader = new Callable<Optional<IBM>>() {
        @Override
        public Optional<IBM> call() throws Exception {
            byte[] primitiveBuffer = new byte[8];
            Optional<IBM> index = delta.replaced ? Optional.<IBM>absent() : backingIndex.getIndex(primitiveBuffer);
            if (index.isPresent()) {
                IBM got = index.get();
                IBM exclude = delta.andNot;
                if (exclude != null) {
                    BM container = bitmaps.create();
                    bitmaps.andNot(container, got, exclude);
                    got = container;
                }
                IBM include = delta.or;
                if (include != null) {
                    BM container = bitmaps.create();
                    bitmaps.or(container, Arrays.asList(got, include));
                    got = container;
                }
                return Optional.of(got);
            } else {
                return Optional.fromNullable(delta.or);
            }
        }
    };

    @Override
    public Optional<IBM> getIndex(byte[] primitiveBuffer) throws Exception {
        if (fieldIndexCache != null) {
            return (Optional<IBM>) fieldIndexCache.get(indexKey, indexLoader);
        } else {
            return indexLoader.call();
        }
    }

    @Override
    public <R> R txIndex(IndexTx<R, IBM> tx, byte[] primitiveBuffer) throws Exception {
        if (fieldIndexCache != null) {
            Optional<IBM> index = (Optional<IBM>) fieldIndexCache.getIfPresent(indexKey);
            if (index != null) {
                LOG.inc("txIndex>cached", 1);
                return tx.tx(index.orNull(), null);
            }
        }

        if (delta.replaced) {
            LOG.inc("txIndex>replaced", 1);
            return tx.tx(delta.or, null);
        } else if (delta.or != null || delta.andNot != null) {
            LOG.inc("txIndex>delta", 1);
            Optional<IBM> index = getIndex(primitiveBuffer);
            return tx.tx(index.orNull(), null);
        } else {
            LOG.inc("txIndex>backing", 1);
            return backingIndex.txIndex(tx, primitiveBuffer);
        }
    }

    private void invalidateCache() {
        if (fieldIndexCache != null) {
            fieldIndexCache.invalidate(indexKey);
        }
        if (versionCache != null) {
            versionCache.invalidate(indexKey);
        }
    }

    @Override
    public Optional<IBM> getIndexUnsafe(byte[] primitiveBuffer) throws Exception {
        return getIndex(primitiveBuffer);
    }

    @Override
    public void replaceIndex(IBM index, int setLastId, byte[] primitiveBuffer) throws Exception {
        synchronized (delta) {
            delta.replaced = true;
            delta.andNot = null;
            delta.or = index;
            delta.lastId = Math.max(delta.lastId, setLastId);
        }
        invalidateCache();
    }

    @Override
    public void append(byte[] primitiveBuffer, int... ids) throws Exception {
        if (ids.length == 0) {
            return;
        }
        synchronized (delta) {

            BM container;
            if (delta.or != null) {
                container = bitmaps.create();
                bitmaps.append(container, delta.or, ids);
            } else {
                container = bitmaps.createWithBits(ids);
            }
            delta.or = container;
            delta.lastId = Math.max(delta.lastId, ids[ids.length - 1]);

        }
        invalidateCache();
    }

    @Override
    public void appendAndExtend(List<Integer> ids, int lastId, byte[] primitiveBuffer) throws Exception {
        if (ids.isEmpty()) {
            return;
        }
        synchronized (delta) {
            BM container;
            if (delta.or != null) {
                container = bitmaps.create();
                bitmaps.extend(container, delta.or, ids, lastId);
            } else {
                container = bitmaps.create();
                bitmaps.extend(container, bitmaps.create(), ids, lastId);
            }
            delta.or = container;
            delta.lastId = Math.max(delta.lastId, lastId);
        }
        invalidateCache();
    }

    @Override
    public void remove(int id, byte[] primitiveBuffer) throws Exception {
        synchronized (delta) {
            if (delta.or != null) {
                BM container = bitmaps.create();
                bitmaps.remove(container, delta.or, id);
                delta.or = container;
            }

            if (delta.andNot != null) {
                BM container = bitmaps.create();
                bitmaps.set(container, delta.andNot, id);
                delta.andNot = container;
            } else {
                delta.andNot = bitmaps.createWithBits(id);
            }
        }
        invalidateCache();
    }

    @Override
    public void set(byte[] primitiveBuffer, int... ids) throws Exception {
        if (ids.length == 0) {
            return;
        }
        synchronized (delta) {
            if (delta.andNot != null) {
                BM container = bitmaps.create();
                bitmaps.remove(container, delta.andNot, ids);
            }

            if (delta.or != null) {
                BM container = bitmaps.create();
                bitmaps.set(container, delta.or, ids);
                delta.or = container;
            } else {
                delta.or = bitmaps.createWithBits(ids);
            }

            for (int id : ids) {
                if (id > delta.lastId) {
                    delta.lastId = id;
                }
            }
        }
        invalidateCache();
    }

    @Override
    public int lastId(byte[] primitiveBuffer) throws Exception {
        if (delta.lastId < 0) {
            delta.lastId = backingIndex.lastId(primitiveBuffer);
        }
        return delta.lastId;
    }

    @Override
    public void andNotToSourceSize(List<IBM> masks, byte[] primitiveBuffer) throws Exception {
        if (masks.isEmpty()) {
            return;
        }
        synchronized (delta) {
            if (delta.or != null) {
                BM container = bitmaps.create();
                bitmaps.andNot(container, delta.or, masks);
                delta.or = container;
            }

            Optional<IBM> index = backingIndex.getIndex(primitiveBuffer);
            if (index.isPresent() || delta.replaced) {
                BM container = bitmaps.create();
                bitmaps.or(container, masks);

                if (!delta.replaced) {
                    // first find the actual bits that matter
                    IBM existing = index.get();
                    BM actualBits = bitmaps.create();
                    //TODO toSourceSize
                    bitmaps.and(actualBits, Arrays.asList(existing, container));
                    container = actualBits;
                }

                if (delta.andNot != null) {
                    BM combined = bitmaps.create();
                    //TODO toSourceSize
                    bitmaps.or(combined, Arrays.asList(delta.andNot, container));
                    container = combined;
                }
                delta.andNot = container;
            } else {
                // if index is empty, andNot is pointless!
                delta.andNot = null;
            }
        }
        invalidateCache();
    }

    @Override
    public void orToSourceSize(IBM mask, byte[] primitiveBuffer) throws Exception {
        synchronized (delta) {
            // technically removing from delta.andNot is unnecessary (since delta.or is applied on top), but reducing its size saves memory
            if (delta.andNot != null) {
                BM container = bitmaps.create();
                bitmaps.andNotToSourceSize(container, delta.andNot, mask);
                delta.andNot = container;
            }

            IBM container;
            if (delta.or != null) {
                BM mutable = bitmaps.create();
                bitmaps.orToSourceSize(mutable, delta.or, mask);
                container = mutable;
            } else {
                container = mask;
            }

            Optional<IBM> index = delta.replaced ? Optional.<IBM>absent() : backingIndex.getIndex(primitiveBuffer);
            if (index.isPresent()) {
                // reduce size of delta
                IBM got = index.get();
                BM actualBits = bitmaps.create();
                bitmaps.andNotToSourceSize(actualBits, container, got);
                container = actualBits;
            }

            delta.or = container;
        }
        invalidateCache();
    }

    @Override
    public void andNot(IBM mask, byte[] primitiveBuffer) throws Exception {
        synchronized (delta) {
            if (delta.or != null) {
                BM container = bitmaps.create();
                bitmaps.andNot(container, delta.or, mask);
                delta.or = container;
            }

            Optional<IBM> index = backingIndex.getIndex(primitiveBuffer);
            if (index.isPresent() || delta.replaced) {
                IBM container = mask;

                if (!delta.replaced) {
                    // first find the actual bits that matter
                    IBM existing = index.get();
                    BM actualBits = bitmaps.create();
                    bitmaps.and(actualBits, Arrays.asList(existing, container));
                    container = actualBits;
                }

                if (delta.andNot != null) {
                    BM combined = bitmaps.create();
                    bitmaps.or(combined, Arrays.asList(delta.andNot, container));
                    container = combined;
                }
                delta.andNot = container;
            } else {
                // if index is empty, andNot is pointless!
                delta.andNot = null;
            }
        }
        invalidateCache();
    }

    @Override
    public void or(IBM mask, byte[] primitiveBuffer) throws Exception {
        synchronized (delta) {
            // technically removing from delta.andNot is unnecessary (since delta.or is applied on top), but reducing its size saves memory
            if (delta.andNot != null) {
                BM container = bitmaps.create();
                bitmaps.andNot(container, delta.andNot, mask);
                delta.andNot = container;
            }

            IBM container;
            if (delta.or != null) {
                BM mutable = bitmaps.create();
                bitmaps.or(mutable, Arrays.asList(delta.or, mask));
                container = mutable;
            } else {
                container = mask;
            }

            Optional<IBM> index = delta.replaced ? Optional.<IBM>absent() : backingIndex.getIndex(primitiveBuffer);
            if (index.isPresent()) {
                // reduce size of delta
                IBM got = index.get();
                BM actualBits = bitmaps.create();
                bitmaps.andNot(actualBits, container, got);
                container = actualBits;
            }

            delta.or = container;
        }
        invalidateCache();
    }

    @Override
    public void merge(byte[] primitiveBuffer) throws Exception {
        if (delta.andNot == null && delta.or == null) {
            return;
        }

        synchronized (delta) {
            Optional<IBM> index = indexLoader.call();
            if (index.isPresent()) {
                backingIndex.replaceIndex(index.get(), Math.max(backingIndex.lastId(primitiveBuffer), delta.lastId), primitiveBuffer);
            }
            delta.lastId = -1;
            delta.andNot = null;
            delta.or = null;
            delta.replaced = false;
        }
    }

    public static class Delta<IBM> {

        private int lastId = -1;
        private IBM andNot;
        private IBM or;
        private boolean replaced = false;
    }
}
