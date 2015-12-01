package com.jivesoftware.os.miru.service.index.delta;

import com.google.common.base.Optional;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.service.index.Mergeable;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Arrays;
import java.util.List;

/**
 * DELTA FORCE
 */
public class MiruDeltaInvertedIndex<BM extends IBM, IBM> implements MiruInvertedIndex<IBM>, Mergeable {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruBitmaps<BM, IBM> bitmaps;
    private final MiruInvertedIndex<IBM> backingIndex;
    private final Delta<IBM> delta;

    public MiruDeltaInvertedIndex(MiruBitmaps<BM, IBM> bitmaps,
        MiruInvertedIndex<IBM> backingIndex,
        Delta<IBM> delta) {
        this.bitmaps = bitmaps;
        this.backingIndex = backingIndex;
        this.delta = delta;
    }

    @Override
    public Optional<IBM> getIndex(StackBuffer stackBuffer) throws Exception {
        boolean replaced;
        IBM or;
        IBM andNot;
        synchronized (delta) {
            replaced = delta.replaced;
            or = delta.or;
            andNot = delta.andNot;
        }
        Optional<IBM> index = replaced ? Optional.<IBM>absent() : backingIndex.getIndex(stackBuffer);
        if (index.isPresent()) {
            IBM got = index.get();
            if (andNot != null) {
                got = bitmaps.andNot(got, andNot);
            }
            if (or != null) {
                got = bitmaps.or(Arrays.asList(got, or));
            }
            return Optional.of(got);
        } else {
            return Optional.fromNullable(or);
        }
    }

    @Override
    public <R> R txIndex(IndexTx<R, IBM> tx, StackBuffer stackBuffer) throws Exception {
        boolean replaced;
        IBM or;
        IBM andNot;
        synchronized (delta) {
            replaced = delta.replaced;
            or = delta.or;
            andNot = delta.andNot;
        }
        if (replaced) {
            LOG.inc("txIndex>replaced", 1);
            return tx.tx(or, null, -1, null);
        } else if (or != null || andNot != null) {
            LOG.inc("txIndex>delta", 1);
            Optional<IBM> index = getIndex(stackBuffer);
            return tx.tx(index.orNull(), null, -1, null);
        } else {
            LOG.inc("txIndex>backing", 1);
            return backingIndex.txIndex(tx, stackBuffer);
        }
    }

    @Override
    public Optional<IBM> getIndexUnsafe(StackBuffer stackBuffer) throws Exception {
        return getIndex(stackBuffer);
    }

    @Override
    public void replaceIndex(IBM index, int setLastId, StackBuffer stackBuffer) throws Exception {
        synchronized (delta) {
            delta.replaced = true;
            delta.andNot = null;
            delta.or = index;
            delta.lastId = Math.max(delta.lastId, setLastId);
        }
    }

    @Override
    public void append(StackBuffer stackBuffer, int... ids) throws Exception {
        if (ids.length == 0) {
            return;
        }
        synchronized (delta) {

            BM container;
            if (delta.or != null) {
                container = bitmaps.append(delta.or, ids);
            } else {
                container = bitmaps.createWithBits(ids);
            }
            delta.or = container;
            delta.lastId = Math.max(delta.lastId, ids[ids.length - 1]);

        }
    }

    @Override
    public void appendAndExtend(List<Integer> ids, int lastId, StackBuffer stackBuffer) throws Exception {
        if (ids.isEmpty()) {
            return;
        }
        synchronized (delta) {
            BM container;
            if (delta.or != null) {
                container = bitmaps.extend(delta.or, ids, lastId);
            } else {
                container = bitmaps.extend(bitmaps.create(), ids, lastId);
            }
            delta.or = container;
            delta.lastId = Math.max(delta.lastId, lastId);
        }
    }

    @Override
    public void remove(int id, StackBuffer stackBuffer) throws Exception {
        synchronized (delta) {
            if (delta.or != null) {
                delta.or = bitmaps.remove(delta.or, id);
            }

            if (delta.andNot != null) {
                delta.andNot = bitmaps.set(delta.andNot, id);
            } else {
                delta.andNot = bitmaps.createWithBits(id);
            }
        }
    }

    @Override
    public void set(StackBuffer stackBuffer, int... ids) throws Exception {
        if (ids.length == 0) {
            return;
        }
        synchronized (delta) {
            if (delta.andNot != null) {
                delta.andNot = bitmaps.remove(delta.andNot, ids);
            }

            if (delta.or != null) {
                delta.or = bitmaps.set(delta.or, ids);
            } else {
                delta.or = bitmaps.createWithBits(ids);
            }

            for (int id : ids) {
                if (id > delta.lastId) {
                    delta.lastId = id;
                }
            }
        }
    }

    @Override
    public int lastId(StackBuffer stackBuffer) throws Exception {
        if (delta.lastId < 0) {
            delta.lastId = backingIndex.lastId(stackBuffer);
        }
        return delta.lastId;
    }

    @Override
    public void andNotToSourceSize(List<IBM> masks, StackBuffer stackBuffer) throws Exception {
        if (masks.isEmpty()) {
            return;
        }
        synchronized (delta) {
            if (delta.or != null) {
                delta.or = bitmaps.andNot(delta.or, masks);
            }

            Optional<IBM> index = backingIndex.getIndex(stackBuffer);
            if (index.isPresent() || delta.replaced) {
                BM container = bitmaps.or(masks);

                if (!delta.replaced) {
                    // first find the actual bits that matter
                    IBM existing = index.get();
                    container = bitmaps.and(Arrays.asList(existing, container));
                }

                if (delta.andNot != null) {
                    container = bitmaps.or(Arrays.asList(delta.andNot, container));
                }
                delta.andNot = container;
            } else {
                // if index is empty, andNot is pointless!
                delta.andNot = null;
            }
        }
    }

    @Override
    public void orToSourceSize(IBM mask, StackBuffer stackBuffer) throws Exception {
        synchronized (delta) {
            // technically removing from delta.andNot is unnecessary (since delta.or is applied on top), but reducing its size saves memory
            if (delta.andNot != null) {
                delta.andNot = bitmaps.andNotToSourceSize(delta.andNot, mask);
            }

            IBM container;
            if (delta.or != null) {
                container = bitmaps.orToSourceSize(delta.or, mask);
            } else {
                container = mask;
            }

            Optional<IBM> index = delta.replaced ? Optional.<IBM>absent() : backingIndex.getIndex(stackBuffer);
            if (index.isPresent()) {
                // reduce size of delta
                IBM got = index.get();
                container = bitmaps.andNotToSourceSize(container, got);
            }

            delta.or = container;
        }
    }

    @Override
    public void andNot(IBM mask, StackBuffer stackBuffer) throws Exception {
        synchronized (delta) {
            if (delta.or != null) {
                delta.or = bitmaps.andNot(delta.or, mask);
            }

            Optional<IBM> index = backingIndex.getIndex(stackBuffer);
            if (index.isPresent() || delta.replaced) {
                IBM container = mask;

                if (!delta.replaced) {
                    // first find the actual bits that matter
                    IBM existing = index.get();
                    container = bitmaps.and(Arrays.asList(existing, container));
                }

                if (delta.andNot != null) {
                    container = bitmaps.or(Arrays.asList(delta.andNot, container));
                }
                delta.andNot = container;
            } else {
                // if index is empty, andNot is pointless!
                delta.andNot = null;
            }
        }
    }

    @Override
    public void or(IBM mask, StackBuffer stackBuffer) throws Exception {
        synchronized (delta) {
            // technically removing from delta.andNot is unnecessary (since delta.or is applied on top), but reducing its size saves memory
            if (delta.andNot != null) {
                delta.andNot = bitmaps.andNot(delta.andNot, mask);
            }

            IBM container;
            if (delta.or != null) {
                container = bitmaps.or(Arrays.asList(delta.or, mask));
            } else {
                container = mask;
            }

            Optional<IBM> index = delta.replaced ? Optional.<IBM>absent() : backingIndex.getIndex(stackBuffer);
            if (index.isPresent()) {
                // reduce size of delta
                IBM got = index.get();
                container = bitmaps.andNot(container, got);
            }

            delta.or = container;
        }
    }

    @Override
    public void merge(StackBuffer stackBuffer) throws Exception {
        if (delta.andNot == null && delta.or == null) {
            return;
        }

        synchronized (delta) {
            Optional<IBM> index = getIndex(stackBuffer);
            if (index.isPresent()) {
                backingIndex.replaceIndex(index.get(), Math.max(backingIndex.lastId(stackBuffer), delta.lastId), stackBuffer);
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
