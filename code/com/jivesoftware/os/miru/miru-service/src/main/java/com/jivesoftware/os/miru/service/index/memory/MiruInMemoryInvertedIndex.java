package com.jivesoftware.os.miru.service.index.memory;

import com.google.common.base.Optional;
import com.jivesoftware.os.filer.map.store.api.KeyValueContext;
import com.jivesoftware.os.filer.map.store.api.KeyValueStore;
import com.jivesoftware.os.filer.map.store.api.KeyValueTransaction;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.BulkImport;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** @author jonathan */
public class MiruInMemoryInvertedIndex<BM> implements MiruInvertedIndex<BM>,
    BulkImport<MiruInvertedIndex<BM>, Void>,
    BulkExport<MiruInvertedIndex<BM>, Void> {

    private final MiruBitmaps<BM> bitmaps;
    private final KeyValueStore<byte[], ReadWrite<BM>> store;
    private final byte[] keyBytes;
    private final int considerIfIndexIdGreaterThanN;

    public MiruInMemoryInvertedIndex(MiruBitmaps<BM> bitmaps,
        KeyValueStore<byte[], ReadWrite<BM>> store,
        byte[] keyBytes,
        int considerIfIndexIdGreaterThanN) {

        this.bitmaps = bitmaps;
        this.store = store;
        this.keyBytes = keyBytes;
        this.considerIfIndexIdGreaterThanN = considerIfIndexIdGreaterThanN;
    }

    @Override
    public Optional<BM> getIndex() throws Exception {
        ReadWrite<BM> readWrite = store.execute(keyBytes, false, new KeyValueTransaction<ReadWrite<BM>, ReadWrite<BM>>() {
            @Override
            public ReadWrite<BM> commit(KeyValueContext<ReadWrite<BM>> keyValueContext) throws IOException {
                return keyValueContext.get();
            }
        });
        if (readWrite != null && (considerIfIndexIdGreaterThanN < 0 || readWrite.lastId > considerIfIndexIdGreaterThanN)) {
            if (readWrite.needsMerge) {
                synchronized (readWrite.write) {
                    merge(readWrite);
                }
            }
            return Optional.fromNullable(readWrite.read.get());
        }
        return Optional.absent();
    }

    @Override
    public Optional<BM> getIndexUnsafe() throws Exception {
        ReadWrite<BM> readWrite = store.execute(keyBytes, false, new KeyValueTransaction<ReadWrite<BM>, ReadWrite<BM>>() {
            @Override
            public ReadWrite<BM> commit(KeyValueContext<ReadWrite<BM>> keyValueContext) throws IOException {
                return keyValueContext.get();
            }
        });
        if (readWrite != null) {
            BM index = readWrite.write.get();
            if (index == null) {
                index = readWrite.read.get();
            }
            return Optional.fromNullable(index);
        }
        return Optional.absent();
    }

    private void markForMerge(ReadWrite<BM> readWrite) {
        readWrite.needsMerge = true;
    }

    /* Synchronize externally */
    private void merge(ReadWrite<BM> readWrite) {
        BM writer = readWrite.write.get();
        if (writer != null) {
            readWrite.read.set(writer);
            readWrite.write.set(null);
        }
        // flip the flag last, since we don't want getIndex() calls to slip through before the merge is finished
        readWrite.needsMerge = false;
    }

    /* Synchronize externally */
    private BM writer(ReadWrite<BM> readWrite) {
        BM bitmap = readWrite.write.get();
        if (bitmap == null) {
            bitmap = copy(readWrite.read.get());
            readWrite.write.set(bitmap);
        }
        return bitmap;
    }

    @Override
    public void append(int... ids) throws Exception {
        if (ids.length == 0) {
            return;
        }
        ReadWrite<BM> readWrite = getOrCreate();
        synchronized (readWrite.write) {
            BM bitmap = writer(readWrite);
            if (!bitmaps.set(bitmap, ids)) {
                throw new RuntimeException("id must be in increasing order"
                    + ", ids = " + Arrays.toString(ids)
                    + ", cardinality = " + bitmaps.cardinality(bitmap)
                    + ", size in bits = " + bitmaps.sizeInBits(bitmap));
            }
            markForMerge(readWrite);

            int appendLastId = ids[ids.length - 1];
            if (appendLastId > readWrite.lastId) {
                readWrite.lastId = appendLastId;
            }
        }
    }

    private ReadWrite<BM> getOrCreate() throws IOException {
        return store.execute(keyBytes, true, new KeyValueTransaction<ReadWrite<BM>, ReadWrite<BM>>() {
            @Override
            public ReadWrite<BM> commit(KeyValueContext<ReadWrite<BM>> keyValueContext) throws IOException {
                ReadWrite<BM> readWrite = keyValueContext.get();
                if (readWrite == null) {
                    readWrite = new ReadWrite<>(bitmaps.create(), -1);
                    keyValueContext.set(readWrite);
                }
                return readWrite;
            }
        });
    }

    @Override
    public void appendAndExtend(List<Integer> ids, int extendToId) throws Exception {
        ReadWrite<BM> readWrite = getOrCreate();
        synchronized (readWrite.write) {
            BM bitmap = writer(readWrite);
            bitmaps.extend(bitmap, ids, extendToId + 1);
            markForMerge(readWrite);

            if (!ids.isEmpty()) {
                int appendLastId = ids.get(ids.size() - 1);
                if (appendLastId > readWrite.lastId) {
                    readWrite.lastId = appendLastId;
                }
            }
        }
    }

    @Override
    public void remove(int id) throws Exception {
        ReadWrite<BM> readWrite = store.execute(keyBytes, false, new KeyValueTransaction<ReadWrite<BM>, ReadWrite<BM>>() {
            @Override
            public ReadWrite<BM> commit(KeyValueContext<ReadWrite<BM>> keyValueContext) throws IOException {
                return keyValueContext.get();
            }
        });
        if (readWrite != null) {
            BM r = bitmaps.create();
            synchronized (readWrite.write) {
                BM remove = bitmaps.create();
                bitmaps.set(remove, id);
                BM bitmap = writer(readWrite);
                bitmaps.andNotToSourceSize(r, bitmap, Collections.singletonList(remove));
                readWrite.write.set(r);
                markForMerge(readWrite);
            }
        }
    }

    @Override
    public void set(int... ids) throws Exception {
        if (ids.length == 0) {
            return;
        }
        ReadWrite<BM> readWrite = getOrCreate();
        synchronized (readWrite.write) {
            BM set = bitmaps.create();
            bitmaps.set(set, ids);
            BM bitmap = writer(readWrite);
            BM r = bitmaps.create();
            bitmaps.or(r, Arrays.asList(bitmap, set));
            readWrite.write.set(r);
            markForMerge(readWrite);

            int setLastId = ids[ids.length - 1];
            if (setLastId > readWrite.lastId) {
                readWrite.lastId = setLastId;
            }
        }
    }

    @Override
    public void setIntermediate(int... ids) throws Exception {
        if (ids.length == 0) {
            return;
        }
        ReadWrite<BM> readWrite = getOrCreate();
        synchronized (readWrite.write) {
            BM bitmap = writer(readWrite);
            BM r = bitmaps.setIntermediate(bitmap, ids);
            readWrite.write.set(r);
            markForMerge(readWrite);

            int setLastId = ids[ids.length - 1];
            if (setLastId > readWrite.lastId) {
                readWrite.lastId = setLastId;
            }
        }
    }

    @Override
    public int lastId() throws Exception {
        ReadWrite<BM> readWrite = store.execute(keyBytes, false, new KeyValueTransaction<ReadWrite<BM>, ReadWrite<BM>>() {
            @Override
            public ReadWrite<BM> commit(KeyValueContext<ReadWrite<BM>> keyValueContext) throws IOException {
                return keyValueContext.get();
            }
        });
        if (readWrite != null) {
            return readWrite.lastId;
        }
        return -1;
    }

    @Override
    public void andNot(BM mask) throws Exception {
        if (bitmaps.isEmpty(mask)) {
            return;
        }
        ReadWrite<BM> readWrite = getOrCreate();
        synchronized (readWrite.write) {
            BM bitmap = writer(readWrite);
            BM r = bitmaps.create();
            bitmaps.andNot(r, bitmap, Collections.singletonList(mask));
            readWrite.write.set(r);
            markForMerge(readWrite);
        }
    }

    @Override
    public void or(BM mask) throws Exception {
        if (bitmaps.isEmpty(mask)) {
            return;
        }
        ReadWrite<BM> readWrite = getOrCreate();
        synchronized (readWrite.write) {
            BM bitmap = writer(readWrite);
            BM r = bitmaps.create();
            bitmaps.or(r, Arrays.asList(bitmap, mask));
            readWrite.write.set(r);
            markForMerge(readWrite);
        }
    }

    @Override
    public void andNotToSourceSize(List<BM> masks) throws Exception {
        if (masks.isEmpty()) {
            return;
        }
        for (BM mask : masks) {
            if (bitmaps.isEmpty(mask)) {
                return;
            }
        }
        ReadWrite<BM> readWrite = getOrCreate();
        BM andNot = bitmaps.create();
        synchronized (readWrite.write) {
            BM bitmap = writer(readWrite);
            bitmaps.andNotToSourceSize(andNot, bitmap, masks);
            readWrite.write.set(andNot);
            markForMerge(readWrite);
        }
    }

    @Override
    public void orToSourceSize(BM mask) throws Exception {
        if (bitmaps.isEmpty(mask)) {
            return;
        }
        ReadWrite<BM> readWrite = getOrCreate();
        BM or = bitmaps.create();
        synchronized (readWrite.write) {
            BM bitmap = writer(readWrite);
            bitmaps.orToSourceSize(or, bitmap, mask);
            readWrite.write.set(or);
            markForMerge(readWrite);
        }
    }

    private BM copy(BM original) {
        //TODO fix BM.clone()
        if (bitmaps.isEmpty(original)) {
            return bitmaps.create();
        } else {
            BM next = bitmaps.create();
            bitmaps.copy(next, original);
            return next;
        }
    }

    @Override
    public MiruInvertedIndex<BM> bulkExport(MiruTenantId tenantId, Void callback) throws Exception {
        return this;
    }

    @Override
    public void bulkImport(MiruTenantId tenantId, BulkExport<MiruInvertedIndex<BM>, Void> export) throws Exception {
        ReadWrite<BM> readWrite = getOrCreate();
        synchronized (readWrite.write) {
            MiruInvertedIndex<BM> invertedIndex = export.bulkExport(tenantId, null);
            readWrite.write.set(invertedIndex.getIndex().get());
            merge(readWrite);
            readWrite.lastId = invertedIndex.lastId();
        }
    }

}
