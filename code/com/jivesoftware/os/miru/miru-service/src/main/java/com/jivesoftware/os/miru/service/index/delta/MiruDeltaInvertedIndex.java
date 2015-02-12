package com.jivesoftware.os.miru.service.index.delta;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * DELTA FORCE
 */
public class MiruDeltaInvertedIndex<BM> implements MiruInvertedIndex<BM> {

    private final MiruBitmaps<BM> bitmaps;
    private final MiruInvertedIndex<BM> backingIndex;
    private final Delta<BM> delta;

    public static class Delta<BM> {

        private int lastId = -1;
        private BM andNot;
        private BM or;
        private boolean replaced = false;
    }

    public void merge() throws Exception {
        if (delta.andNot == null && delta.or == null) {
            return;
        }

        synchronized (delta) {
            Optional<BM> index = getIndex();
            if (index.isPresent()) {
                backingIndex.replaceIndex(index.get(), Math.max(backingIndex.lastId(), delta.lastId));
            }
            delta.lastId = -1;
            delta.andNot = null;
            delta.or = null;
            delta.replaced = false;
        }
    }

    public MiruDeltaInvertedIndex(MiruBitmaps<BM> bitmaps,
        MiruInvertedIndex<BM> backingIndex,
        Delta<BM> delta) {
        this.bitmaps = bitmaps;
        this.backingIndex = backingIndex;
        this.delta = delta;
    }

    @Override
    public Optional<BM> getIndex() throws Exception {
        Optional<BM> index = delta.replaced ? Optional.<BM>absent() : backingIndex.getIndex();
        if (index.isPresent()) {
            BM got = index.get();
            BM exclude = delta.andNot;
            if (exclude != null) {
                BM container = bitmaps.create();
                bitmaps.andNot(container, got, Collections.singletonList(exclude));
                got = container;
            }
            BM include = delta.or;
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

    @Override
    public Optional<BM> getIndexUnsafe() throws Exception {
        return getIndex();
    }

    @Override
    public void replaceIndex(BM index, int setLastId) throws Exception {
        synchronized (delta) {
            delta.replaced = true;
            delta.andNot = null;
            delta.or = index;
            delta.lastId = Math.max(delta.lastId, setLastId);
        }
    }

    @Override
    public void append(int... ids) throws Exception {
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
    }

    @Override
    public void appendAndExtend(List<Integer> ids, int lastId) throws Exception {
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
    }

    @Override
    public void remove(int id) throws Exception {
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
    }

    @Override
    public void set(int... ids) throws Exception {
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
    }

    @Override
    public int lastId() throws Exception {
        if (delta.lastId < 0) {
            delta.lastId = backingIndex.lastId();
        }
        return delta.lastId;
    }

    @Override
    public void andNotToSourceSize(List<BM> masks) throws Exception {
        if (masks.isEmpty()) {
            return;
        }
        synchronized (delta) {
            if (delta.or != null) {
                BM container = bitmaps.create();
                bitmaps.andNot(container, delta.or, masks);
                delta.or = container;
            }

            Optional<BM> index = backingIndex.getIndex();
            if (index.isPresent() || delta.replaced) {
                BM container = bitmaps.create();
                bitmaps.or(container, masks);

                if (!delta.replaced) {
                    // first find the actual bits that matter
                    BM existing = index.get();
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
    }

    @Override
    public void orToSourceSize(BM mask) throws Exception {
        synchronized (delta) {
            // technically removing from delta.andNot is unnecessary (since delta.or is applied on top), but reducing its size saves memory
            if (delta.andNot != null) {
                BM container = bitmaps.create();
                bitmaps.andNotToSourceSize(container, delta.andNot, Collections.singletonList(mask));
                delta.andNot = container;
            }

            BM container;
            if (delta.or != null) {
                container = bitmaps.create();
                bitmaps.orToSourceSize(container, delta.or, mask);
            } else {
                container = mask;
            }

            Optional<BM> index = delta.replaced ? Optional.<BM>absent() : backingIndex.getIndex();
            if (index.isPresent()) {
                // reduce size of delta
                BM got = index.get();
                BM actualBits = bitmaps.create();
                bitmaps.andNotToSourceSize(actualBits, container, Collections.singletonList(got));
                container = actualBits;
            }

            delta.or = container;
        }
    }

    @Override
    public void andNot(BM mask) throws Exception {
        synchronized (delta) {
            if (delta.or != null) {
                BM container = bitmaps.create();
                bitmaps.andNot(container, delta.or, Collections.singletonList(mask));
                delta.or = container;
            }

            Optional<BM> index = backingIndex.getIndex();
            if (index.isPresent() || delta.replaced) {
                BM container = mask;

                if (!delta.replaced) {
                    // first find the actual bits that matter
                    BM existing = index.get();
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
    }

    @Override
    public void or(BM mask) throws Exception {
        synchronized (delta) {
            // technically removing from delta.andNot is unnecessary (since delta.or is applied on top), but reducing its size saves memory
            if (delta.andNot != null) {
                BM container = bitmaps.create();
                bitmaps.andNot(container, delta.andNot, Collections.singletonList(mask));
                delta.andNot = container;
            }

            BM container;
            if (delta.or != null) {
                container = bitmaps.create();
                bitmaps.or(container, Arrays.asList(delta.or, mask));
            } else {
                container = mask;
            }

            Optional<BM> index = delta.replaced ? Optional.<BM>absent() : backingIndex.getIndex();
            if (index.isPresent()) {
                // reduce size of delta
                BM got = index.get();
                BM actualBits = bitmaps.create();
                bitmaps.andNot(actualBits, container, Collections.singletonList(got));
                container = actualBits;
            }

            delta.or = container;
        }
    }
}
