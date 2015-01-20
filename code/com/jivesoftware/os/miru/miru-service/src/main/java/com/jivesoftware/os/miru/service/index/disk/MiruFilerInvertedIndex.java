package com.jivesoftware.os.miru.service.index.disk;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.FilerTransaction;
import com.jivesoftware.os.filer.io.RewriteFilerTransaction;
import com.jivesoftware.os.filer.keyed.store.KeyedFilerStore;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.service.index.BitmapAndLastId;
import java.io.DataInput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author jonathan
 */
public class MiruFilerInvertedIndex<BM> implements MiruInvertedIndex<BM> {

    private static final int LAST_ID_LENGTH = 4;

    private final MiruBitmaps<BM> bitmaps;
    private final KeyedFilerStore keyedFilerStore;
    private final byte[] keyBytes;
    private final int considerIfIndexIdGreaterThanN;
    private final Object mutationLock;
    private volatile int lastId = Integer.MIN_VALUE;

    public MiruFilerInvertedIndex(MiruBitmaps<BM> bitmaps,
        KeyedFilerStore keyedFilerStore,
        byte[] keyBytes,
        int considerIfIndexIdGreaterThanN,
        Object mutationLock) {
        this.bitmaps = bitmaps;
        this.keyedFilerStore = Preconditions.checkNotNull(keyedFilerStore);
        this.keyBytes = Preconditions.checkNotNull(keyBytes);
        this.considerIfIndexIdGreaterThanN = considerIfIndexIdGreaterThanN;
        this.mutationLock = mutationLock;
    }

    @Override
    public Optional<BM> getIndex() throws Exception {
        if (lastId > Integer.MIN_VALUE && lastId <= considerIfIndexIdGreaterThanN) {
            return Optional.absent();
        }
        byte[] rawBytes = keyedFilerStore.execute(keyBytes, -1, getTransaction);
        BitmapAndLastId<BM> bitmapAndLastId = deser(rawBytes);
        if (bitmapAndLastId != null) {
            if (lastId == Integer.MIN_VALUE) {
                lastId = bitmapAndLastId.lastId;
            }
            return Optional.of(bitmapAndLastId.bitmap);
        } else {
            lastId = -1;
            return Optional.absent();
        }
    }

    private BitmapAndLastId<BM> deser(byte[] bytes) throws IOException {
        //TODO just add a byte marker, this sucks
        if (bytes != null && bytes.length > LAST_ID_LENGTH + 4) {
            if (FilerIO.bytesInt(bytes, LAST_ID_LENGTH) > 0) {
                int lastId = FilerIO.bytesInt(bytes, 0);
                DataInput dataInput = ByteStreams.newDataInput(bytes, LAST_ID_LENGTH);
                try {
                    return new BitmapAndLastId<>(bitmaps.deserialize(dataInput), lastId);
                } catch (Exception e) {
                    throw new IOException("Failed to deserialize", e);
                }
            } else {
                return new BitmapAndLastId<>(bitmaps.create(), -1);
            }
        }
        return null;
    }

    private BM getOrCreateIndex() throws Exception {
        Optional<BM> index = getIndex();
        return index.isPresent() ? index.get() : bitmaps.create();
    }

    private static <BM> long serializedSizeInBytes(MiruBitmaps<BM> bitmaps, BM index) {
        return LAST_ID_LENGTH + bitmaps.serializedSizeInBytes(index);
    }

    private void setIndex(BM index, int setLastId) throws Exception {
        long filerSizeInBytes = serializedSizeInBytes(bitmaps, index);
        ByteArrayDataOutput dataOutput = ByteStreams.newDataOutput((int) filerSizeInBytes);
        dataOutput.write(FilerIO.intBytes(setLastId));
        bitmaps.serialize(index, dataOutput);
        final byte[] bytes = dataOutput.toByteArray();
        keyedFilerStore.executeRewrite(keyBytes, filerSizeInBytes, new SetTransaction(bytes));
    }

    @Override
    public Optional<BM> getIndexUnsafe() throws Exception {
        return getIndex();
    }

    @Override
    public void append(int... ids) throws Exception {
        if (ids.length == 0) {
            return;
        }
        synchronized (mutationLock) {
            BM index = getOrCreateIndex();
            if (!bitmaps.set(index, ids)) {
                throw new RuntimeException("ids must be in increasing order"
                    + ", ids = " + Arrays.toString(ids)
                    + ", cardinality = " + bitmaps.cardinality(index)
                    + ", size in bits = " + bitmaps.sizeInBits(index));
            }

            int appendLastId = ids[ids.length - 1];
            if (appendLastId > lastId) {
                lastId = appendLastId;
            }

            setIndex(index, lastId);
        }
    }

    @Override
    public void appendAndExtend(List<Integer> ids, int extendToId) throws Exception {
        synchronized (mutationLock) {
            BM index = getOrCreateIndex();
            bitmaps.extend(index, ids, extendToId + 1);

            if (!ids.isEmpty()) {
                int appendLastId = ids.get(ids.size() - 1);
                if (appendLastId > lastId) {
                    lastId = appendLastId;
                }
            }

            setIndex(index, lastId);
        }
    }

    @Override
    public void remove(int id) throws Exception { // Kinda crazy expensive way to remove an intermediary bit.
        BM remove = bitmaps.create();
        bitmaps.set(remove, id);
        synchronized (mutationLock) {
            BM index = getOrCreateIndex();
            BM r = bitmaps.create();
            bitmaps.andNot(r, index, Collections.singletonList(remove));
            setIndex(r, lastId);
        }
    }

    @Override
    public void set(int... ids) throws Exception { // Kinda crazy expensive way to set an intermediary bit.
        if (ids.length == 0) {
            return;
        }
        BM set = bitmaps.create();
        bitmaps.set(set, ids);
        synchronized (mutationLock) {
            BM index = getOrCreateIndex();
            BM r = bitmaps.create();
            bitmaps.or(r, Arrays.asList(index, set));

            int setLastId = ids[ids.length - 1];
            if (setLastId > lastId) {
                lastId = setLastId;
            }

            setIndex(r, lastId);
        }
    }

    @Override
    public void setIntermediate(int... ids) throws Exception {
        if (ids.length == 0) {
            return;
        }
        synchronized (mutationLock) {
            BM index = getOrCreateIndex();
            BM r = bitmaps.setIntermediate(index, ids);

            int setLastId = ids[ids.length - 1];
            if (setLastId > lastId) {
                lastId = setLastId;
            }

            setIndex(r, lastId);
        }
    }

    @Override
    public int lastId() throws Exception {
        if (lastId == Integer.MIN_VALUE) {
            synchronized (mutationLock) {
                lastId = keyedFilerStore.execute(keyBytes, -1, lastIdTransaction);
            }
        }
        return lastId;
    }

    private static int getLastId(Object lock, Filer filer) throws IOException {
        synchronized (lock) {
            filer.seek(0);
            return FilerIO.readInt(filer, "lastId");
        }
    }

    @Override
    public void andNot(BM mask) throws Exception {
        synchronized (mutationLock) {
            BM index = getOrCreateIndex();
            BM r = bitmaps.create();
            bitmaps.andNot(r, index, Collections.singletonList(mask));
            setIndex(r, lastId);
        }
    }

    @Override
    public void or(BM mask) throws Exception {
        synchronized (mutationLock) {
            BM index = getOrCreateIndex();
            BM r = bitmaps.create();
            bitmaps.or(r, Arrays.asList(index, mask));
            setIndex(r, Math.max(lastId, bitmaps.lastSetBit(mask)));
        }
    }

    @Override
    public void andNotToSourceSize(List<BM> masks) throws Exception {
        synchronized (mutationLock) {
            BM index = getOrCreateIndex();
            BM andNot = bitmaps.create();
            bitmaps.andNotToSourceSize(andNot, index, masks);
            setIndex(andNot, lastId);
        }
    }

    @Override
    public void orToSourceSize(BM mask) throws Exception {
        synchronized (mutationLock) {
            BM index = getOrCreateIndex();
            BM or = bitmaps.create();
            bitmaps.orToSourceSize(or, index, mask);
            setIndex(or, lastId);
        }
    }

    private static final FilerTransaction<Filer, Integer> lastIdTransaction = new FilerTransaction<Filer, Integer>() {
        @Override
        public Integer commit(Object lock, Filer filer) throws IOException {
            if (filer != null) {
                return getLastId(lock, filer);
            } else {
                return -1;
            }
        }
    };

    private static final FilerTransaction<Filer, byte[]> getTransaction = new FilerTransaction<Filer, byte[]>() {

        public byte[] commit(Object lock, Filer filer) throws IOException {
            if (filer != null) {
                synchronized (lock) {
                    filer.seek(0);
                    byte[] bytes = new byte[(int) filer.length()];
                    FilerIO.read(filer, bytes);
                    return bytes;
                }
            }
            return null;
        }
    };

    private static class SetTransaction implements RewriteFilerTransaction<Filer, Void> {

        private final byte[] bytes;

        private SetTransaction(byte[] bytes) {
            this.bytes = bytes;
        }

        @Override
        public Void commit(Filer currentFiler, Filer newFiler) throws IOException {
            newFiler.seek(0);
            newFiler.write(bytes);
            return null;
        }
    }
}
