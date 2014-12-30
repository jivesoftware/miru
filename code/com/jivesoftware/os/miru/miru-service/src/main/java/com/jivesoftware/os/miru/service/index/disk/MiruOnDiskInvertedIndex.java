package com.jivesoftware.os.miru.service.index.disk;

import com.google.common.base.Optional;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.FilerTransaction;
import com.jivesoftware.os.filer.io.RewriteFilerTransaction;
import com.jivesoftware.os.filer.keyed.store.KeyedFilerStore;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.service.index.BitmapAndLastId;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.BulkImport;
import java.io.DataInput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** @author jonathan */
public class MiruOnDiskInvertedIndex<BM> implements MiruInvertedIndex<BM>,
    BulkImport<MiruInvertedIndex<BM>, Void>,
    BulkExport<MiruInvertedIndex<BM>, Void> {

    private static final int LAST_ID_LENGTH = 4;

    private final MiruBitmaps<BM> bitmaps;
    private final KeyedFilerStore keyedFilerStore;
    private final byte[] keyBytes;
    private final int considerIfIndexIdGreaterThanN;
    private final long initialCapacityInBytes;
    private final Object mutationLock;
    private volatile int lastId = Integer.MIN_VALUE;

    public MiruOnDiskInvertedIndex(MiruBitmaps<BM> bitmaps,
        KeyedFilerStore keyedFilerStore,
        byte[] keyBytes,
        int considerIfIndexIdGreaterThanN,
        long initialCapacityInBytes,
        Object mutationLock) {
        this.bitmaps = bitmaps;
        this.keyedFilerStore = keyedFilerStore;
        this.keyBytes = keyBytes;
        this.considerIfIndexIdGreaterThanN = considerIfIndexIdGreaterThanN;
        this.initialCapacityInBytes = initialCapacityInBytes;
        this.mutationLock = mutationLock;
    }

    @Override
    public Optional<BM> getIndex() throws Exception {
        return getIndex(initialCapacityInBytes);
    }

    private Optional<BM> getIndex(long capacity) throws Exception {
        if (lastId > Integer.MIN_VALUE && lastId <= considerIfIndexIdGreaterThanN) {
            return null;
        }
        //TODO transaction should be passed in since it only depends on 'bitmaps'
        BitmapAndLastId<BM> bitmapAndLastId = keyedFilerStore.execute(keyBytes, capacity, new GetWithLastIdTransaction<>(bitmaps));
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

    public static <BM> long serializedSizeInBytes(MiruBitmaps<BM> bitmaps, BM index) {
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
            Optional<BM> index = getIndex();
            if (index.isPresent()) {
                if (!bitmaps.set(index.get(), ids)) {
                    throw new RuntimeException("ids must be in increasing order"
                        + ", ids = " + Arrays.toString(ids)
                        + ", cardinality = " + bitmaps.cardinality(index.get())
                        + ", size in bits = " + bitmaps.sizeInBits(index.get()));
                }

                int appendLastId = ids[ids.length - 1];
                if (appendLastId > lastId) {
                    lastId = appendLastId;
                }

                setIndex(index.get(), lastId);
            }
        }
    }

    @Override
    public void appendAndExtend(List<Integer> ids, int extendToId) throws Exception {
        synchronized (mutationLock) {
            Optional<BM> index = getIndex();
            if (index.isPresent()) {
                bitmaps.extend(index.get(), ids, extendToId + 1);

                if (!ids.isEmpty()) {
                    int appendLastId = ids.get(ids.size() - 1);
                    if (appendLastId > lastId) {
                        lastId = appendLastId;
                    }
                }

                setIndex(index.get(), lastId);
            }
        }
    }

    @Override
    public void remove(int id) throws Exception { // Kinda crazy expensive way to remove an intermediary bit.
        BM remove = bitmaps.create();
        bitmaps.set(remove, id);
        synchronized (mutationLock) {
            Optional<BM> index = getIndex();
            if (index.isPresent()) {
                BM r = bitmaps.create();
                bitmaps.andNot(r, index.get(), Collections.singletonList(remove));
                setIndex(r, lastId);
            }
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
            Optional<BM> index = getIndex();
            if (index.isPresent()) {
                BM r = bitmaps.create();
                bitmaps.or(r, Arrays.asList(index.get(), set));

                int setLastId = ids[ids.length - 1];
                if (setLastId > lastId) {
                    lastId = setLastId;
                }

                setIndex(r, lastId);
            }
        }
    }

    @Override
    public void setIntermediate(int... ids) throws Exception {
        if (ids.length == 0) {
            return;
        }
        synchronized (mutationLock) {
            Optional<BM> index = getIndex();
            if (index.isPresent()) {
                BM r = bitmaps.setIntermediate(index.get(), ids);

                int setLastId = ids[ids.length - 1];
                if (setLastId > lastId) {
                    lastId = setLastId;
                }

                setIndex(r, lastId);
            }
        }
    }

    @Override
    public int lastId() throws Exception {
        if (lastId == Integer.MIN_VALUE) {
            lastId = keyedFilerStore.execute(keyBytes, initialCapacityInBytes, new FilerTransaction<Filer, Integer>() {
                @Override
                public Integer commit(Filer filer) throws IOException {
                    if (filer != null) {
                        return getLastId(filer);
                    } else {
                        return Integer.MIN_VALUE;
                    }
                }
            });
        }
        return lastId;
    }

    private int getLastId(Filer filer) throws IOException {
        filer.seek(0);
        return FilerIO.readInt(filer, "lastId");
    }

    @Override
    public void andNot(BM mask) throws Exception {
        synchronized (mutationLock) {
            Optional<BM> index = getIndex();
            if (index.isPresent()) {
                BM r = bitmaps.create();
                bitmaps.andNot(r, index.get(), Collections.singletonList(mask));
                setIndex(r, lastId);
            }
        }
    }

    @Override
    public void or(BM mask) throws Exception {
        synchronized (mutationLock) {
            Optional<BM> index = getIndex();
            if (index.isPresent()) {
                BM r = bitmaps.create();
                bitmaps.or(r, Arrays.asList(index.get(), mask));
                setIndex(r, lastId);
            }
        }
    }

    @Override
    public void andNotToSourceSize(List<BM> masks) throws Exception {
        synchronized (mutationLock) {
            Optional<BM> index = getIndex();
            if (index.isPresent()) {
                BM andNot = bitmaps.create();
                bitmaps.andNotToSourceSize(andNot, index.get(), masks);
                setIndex(andNot, lastId);
            }
        }
    }

    @Override
    public void orToSourceSize(BM mask) throws Exception {
        synchronized (mutationLock) {
            Optional<BM> index = getIndex();
            if (index.isPresent()) {
                BM or = bitmaps.create();
                bitmaps.orToSourceSize(or, index.get(), mask);
                setIndex(or, lastId);
            }
        }
    }

    @Override
    public long sizeInMemory() throws Exception {
        return 0;
    }

    @Override
    public long sizeOnDisk() throws Exception {
        return 0;
    }

    @Override
    public void bulkImport(MiruTenantId tenantId, BulkExport<MiruInvertedIndex<BM>, Void> export) throws Exception {
        MiruInvertedIndex<BM> exportIndex = export.bulkExport(tenantId, null);
        setIndex(exportIndex.getIndex().get(), exportIndex.lastId());
    }

    @Override
    public MiruInvertedIndex<BM> bulkExport(MiruTenantId tenantId, Void callback) throws Exception {
        return this;
    }

    private static class GetWithLastIdTransaction<BM> implements FilerTransaction<Filer, BitmapAndLastId<BM>> {

        private final MiruBitmaps<BM> bitmaps;

        private GetWithLastIdTransaction(MiruBitmaps<BM> bitmaps) {
            this.bitmaps = bitmaps;
        }

        @Override
        public BitmapAndLastId<BM> commit(Filer filer) throws IOException {
            if (filer != null) {
                filer.seek(0);
                byte[] bytes = new byte[(int) filer.length()];
                FilerIO.read(filer, bytes);
                //TODO just add a byte marker, this sucks
                if (bytes.length > LAST_ID_LENGTH + 4 && FilerIO.bytesInt(bytes, LAST_ID_LENGTH) > 0) {
                    int lastId = FilerIO.bytesInt(bytes, 0);
                    DataInput dataInput = ByteStreams.newDataInput(bytes, LAST_ID_LENGTH);
                    try {
                        return new BitmapAndLastId<>(bitmaps.deserialize(dataInput), lastId);
                    } catch (Exception e) {
                        throw new IOException("Failed to deserialize", e);
                    }
                }
            }
            return null;
        }
    }

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
