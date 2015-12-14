package com.jivesoftware.os.miru.service.index.delta;

import com.google.common.base.Optional;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.BitmapAndLastId;
import com.jivesoftware.os.miru.plugin.index.IndexTx;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.plugin.partition.TrackError;
import com.jivesoftware.os.miru.service.index.Mergeable;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Arrays;
import java.util.List;

/**
 * DELTA FORCE
 */
public class MiruDeltaInvertedIndex<BM extends IBM, IBM> implements MiruInvertedIndex<BM, IBM>, Mergeable {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruBitmaps<BM, IBM> bitmaps;
    private final TrackError trackError;
    private final MiruInvertedIndex<BM, IBM> backingIndex;
    private final Delta<IBM> delta;

    public MiruDeltaInvertedIndex(MiruBitmaps<BM, IBM> bitmaps,
        TrackError trackError,
        MiruInvertedIndex<BM, IBM> backingIndex,
        Delta<IBM> delta) {
        this.bitmaps = bitmaps;
        this.trackError = trackError;
        this.backingIndex = backingIndex;
        this.delta = delta;
    }

    @Override
    public Optional<BM> getIndex(StackBuffer stackBuffer) throws Exception {
        return overlayDelta(bitmaps, delta, backingIndex, -1, trackError, stackBuffer).transform(input -> input.bitmap);
    }

    @Override
    public Optional<BitmapAndLastId<BM>> getIndexAndLastId(int considerIfLastIdGreaterThanN, StackBuffer stackBuffer) throws Exception {
        return overlayDelta(bitmaps, delta, backingIndex, considerIfLastIdGreaterThanN, trackError, stackBuffer);
    }

    public static <BM extends IBM, IBM> Optional<BitmapAndLastId<BM>> overlayDelta(MiruBitmaps<BM, IBM> bitmaps,
        Delta<IBM> delta,
        MiruInvertedIndex<BM, IBM> backingIndex,
        int considerIfLastIdGreaterThanN,
        TrackError trackError,
        StackBuffer stackBuffer) throws Exception {

        int lastId = -1;
        boolean replaced;
        boolean ifEmpty;
        IBM or;
        IBM andNot;
        synchronized (delta) {
            lastId = delta.lastId;
            replaced = delta.replaced;
            ifEmpty = delta.ifEmpty;
            or = delta.or;
            andNot = delta.andNot;
        }

        int backingLastIdGreaterThanN = (lastId > considerIfLastIdGreaterThanN) ? -1 : considerIfLastIdGreaterThanN;
        Optional<BitmapAndLastId<BM>> index = replaced ? Optional.<BitmapAndLastId<BM>>absent()
            : backingIndex.getIndexAndLastId(backingLastIdGreaterThanN, stackBuffer);
        if (index.isPresent()) {
            BitmapAndLastId<BM> got = index.get();
            if (bitmaps.isEmpty(got.bitmap)) {
                trackError.error("Delta backed by empty bitmap," +
                    " andNot:" + (andNot != null ? bitmaps.cardinality(andNot) : -1) +
                    " or:" + (or != null ? bitmaps.cardinality(or) : -1));
            }
            got = overlay(bitmaps, got, ifEmpty, or, andNot, lastId);
            if (bitmaps.isEmpty(got.bitmap)) {
                trackError.error("Delta merged to empty bitmap," +
                    " andNot:" + (andNot != null ? bitmaps.cardinality(andNot) : -1) +
                    " or:" + (or != null ? bitmaps.cardinality(or) : -1));
            }
            return Optional.of(got);
        } else if (or != null && (lastId > considerIfLastIdGreaterThanN)) {
            return Optional.of(new BitmapAndLastId<>(bitmaps.copy(or), lastId));
        } else {
            return Optional.absent();
        }
    }

    public static <BM extends IBM, IBM> BitmapAndLastId<BM> overlay(MiruBitmaps<BM, IBM> bitmaps,
        BitmapAndLastId<BM> backing,
        boolean ifEmpty,
        IBM or,
        IBM andNot,
        int lastId) {

        BitmapAndLastId<BM> overlayed = backing;
        if (!ifEmpty || backing.lastId < 0) {
            BM got = backing.bitmap;
            if (andNot != null) {
                got = bitmaps.andNot(got, andNot);
            }
            if (or != null) {
                got = bitmaps.or(Arrays.asList(got, or));
            }
            overlayed = new BitmapAndLastId<>(got, Math.max(lastId, backing.lastId));
        }
        return overlayed;
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
            return tx.tx(or != null ? bitmaps.copy(or) : null, null, -1, null);
        } else if (or != null || andNot != null) {
            LOG.inc("txIndex>delta", 1);
            Optional<BM> index = getIndex(stackBuffer);
            return tx.tx(index.orNull(), null, -1, null);
        } else {
            LOG.inc("txIndex>backing", 1);
            return backingIndex.txIndex(tx, stackBuffer);
        }
    }

    @Override
    public void replaceIndex(IBM index, int setLastId, StackBuffer stackBuffer) throws Exception {
        synchronized (delta) {
            clearIfEmpty();

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
            clearIfEmpty();

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
            clearIfEmpty();

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
            clearIfEmpty();

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
            clearIfEmpty();

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
    public void setIfEmpty(StackBuffer stackBuffer, int id) throws Exception {
        synchronized (delta) {
            if (delta.lastId == -1 && delta.or == null && delta.andNot == null && !delta.replaced && !delta.ifEmpty) {
                delta.lastId = id;
                delta.or = bitmaps.createWithBits(id);
                delta.ifEmpty = true;
            }
        }
    }

    private void clearIfEmpty() {
        if (delta.ifEmpty) {
            delta.ifEmpty = false;
            delta.or = null;
        }
    }

    @Override
    public int lastId(StackBuffer stackBuffer) throws Exception {
        return Math.max(delta.lastId, backingIndex.lastId(stackBuffer));
    }

    @Override
    public void andNotToSourceSize(List<IBM> masks, StackBuffer stackBuffer) throws Exception {
        if (masks.isEmpty()) {
            return;
        }
        synchronized (delta) {
            clearIfEmpty();

            if (delta.or != null) {
                delta.or = bitmaps.andNot(delta.or, masks);
            }

            Optional<BM> index = backingIndex.getIndex(stackBuffer);
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
            clearIfEmpty();

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

            Optional<BM> index = delta.replaced ? Optional.<BM>absent() : backingIndex.getIndex(stackBuffer);
            if (index.isPresent()) {
                // reduce size of delta
                BM got = index.get();
                container = bitmaps.andNotToSourceSize(container, got);
            }

            delta.or = container;
        }
    }

    @Override
    public void andNot(IBM mask, StackBuffer stackBuffer) throws Exception {
        synchronized (delta) {
            clearIfEmpty();

            if (delta.or != null) {
                delta.or = bitmaps.andNot(delta.or, mask);
            }

            Optional<BM> index = backingIndex.getIndex(stackBuffer);
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
            clearIfEmpty();

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

            Optional<BM> index = delta.replaced ? Optional.<BM>absent() : backingIndex.getIndex(stackBuffer);
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
            Optional<BM> index = getIndex(stackBuffer);
            if (index.isPresent()) {
                backingIndex.replaceIndex(index.get(), Math.max(backingIndex.lastId(stackBuffer), delta.lastId), stackBuffer);
            }
            delta.lastId = -1;
            delta.andNot = null;
            delta.or = null;
            delta.replaced = false;
            delta.ifEmpty = false;
        }
    }

    public static class Delta<IBM> {

        public int lastId = -1;
        public IBM andNot;
        public IBM or;
        public boolean replaced = false;
        public boolean ifEmpty = false;
    }
}
