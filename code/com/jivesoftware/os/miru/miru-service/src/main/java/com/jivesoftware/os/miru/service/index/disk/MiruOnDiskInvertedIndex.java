package com.jivesoftware.os.miru.service.index.disk;

import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.keyed.store.SwappableFiler;
import com.jivesoftware.os.filer.keyed.store.SwappingFiler;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.BulkImport;
import java.io.DataInput;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** @author jonathan */
public class MiruOnDiskInvertedIndex<BM> implements MiruInvertedIndex<BM>, BulkImport<BM> {

    private final MiruBitmaps<BM> bitmaps;
    private final SwappableFiler filer;
    private final int startPosition;

    public MiruOnDiskInvertedIndex(MiruBitmaps<BM> bitmaps, SwappableFiler filer) {
        this(bitmaps, filer, 0);
    }

    public MiruOnDiskInvertedIndex(MiruBitmaps<BM> bitmaps, SwappableFiler filer, int startPosition) {
        this.bitmaps = bitmaps;
        this.filer = filer;
        this.startPosition = startPosition;
    }

    @Override
    public BM getIndex() throws Exception {
        synchronized (filer.lock()) {
            filer.sync();
            filer.seek(startPosition);
            DataInput dataInput = FilerIO.asDataInput(filer);
            if (dataInput.readInt() > 0) {
                filer.seek(startPosition);
                return bitmaps.deserialize(dataInput);
            }
        }
        return bitmaps.create();
    }

    @Override
    public BM getIndexUnsafe() throws Exception {
        return getIndex();
    }

    @Override
    public void append(int... ids) throws Exception {
        synchronized (filer.lock()) {
            filer.sync();
            BM index = getIndex();
            if (!bitmaps.set(index, ids)) {
                throw new RuntimeException("ids must be in increasing order"
                    + ", ids = " + Arrays.toString(ids)
                    + ", cardinality = " + bitmaps.cardinality(index)
                    + ", size in bits = " + bitmaps.sizeInBits(index));
            }
            setIndex(index);
        }
    }

    @Override
    public void appendAndExtend(List<Integer> ids, int lastId) throws Exception {
        synchronized (filer.lock()) {
            filer.sync();
            BM index = getIndex();
            bitmaps.extend(index, ids, lastId + 1);
            setIndex(index);
        }
    }

    @Override
    public void remove(int id) throws Exception { // Kinda crazy expensive way to remove an intermediary bit.
        BM remove = bitmaps.create();
        bitmaps.set(remove, id);
        synchronized (filer.lock()) {
            filer.sync();
            BM r = bitmaps.create();
            bitmaps.andNot(r, getIndex(), Collections.singletonList(remove));
            setIndex(r);
        }
    }

    @Override
    public void set(int... ids) throws Exception { // Kinda crazy expensive way to set an intermediary bit.
        BM set = bitmaps.create();
        bitmaps.set(set, ids);
        synchronized (filer.lock()) {
            filer.sync();
            BM r = bitmaps.create();
            bitmaps.or(r, Arrays.asList(getIndex(), set));
            setIndex(r);
        }
    }

    @Override
    public void setIntermediate(int... ids) throws Exception {
        synchronized (filer.lock()) {
            filer.sync();
            BM r = bitmaps.setIntermediate(getIndex(), ids);
            setIndex(r);
        }
    }

    @Override
    public void andNot(BM mask) throws Exception {
        synchronized (filer.lock()) {
            filer.sync();
            BM r = bitmaps.create();
            bitmaps.andNot(r, getIndex(), Collections.singletonList(mask));
            setIndex(r);
        }
    }

    @Override
    public void or(BM mask) throws Exception {
        synchronized (filer.lock()) {
            filer.sync();
            BM r = bitmaps.create();
            bitmaps.or(r, Arrays.asList(getIndex(), mask));
            setIndex(r);
        }
    }

    @Override
    public void andNotToSourceSize(List<BM> masks) throws Exception {
        synchronized (filer.lock()) {
            filer.sync();
            BM andNot = bitmaps.create();
            bitmaps.andNotToSourceSize(andNot, getIndex(), masks);
            setIndex(andNot);
        }
    }

    @Override
    public void orToSourceSize(BM mask) throws Exception {
        synchronized (filer.lock()) {
            filer.sync();
            BM or = bitmaps.create();
            bitmaps.orToSourceSize(or, getIndex(), mask);
            setIndex(or);
        }
    }

    @Override
    public long sizeInMemory() throws Exception {
        return 0;
    }

    @Override
    public long sizeOnDisk() throws Exception {
        return filer.length();
    }

    private void setIndex(BM index) throws Exception {
        synchronized (filer.lock()) {
            filer.sync();
            filer.seek(0);
            byte[] initialBytes = new byte[startPosition];
            FilerIO.read(filer, initialBytes);

            SwappingFiler swap = filer.swap(startPosition + bitmaps.serializedSizeInBytes(index));
            FilerIO.write(swap, initialBytes);
            bitmaps.serialize(index, FilerIO.asDataOutput(swap));
            swap.commit();
        }
    }

    @Override
    public void bulkImport(MiruTenantId tenantId, BulkExport<BM> importItems) throws Exception {
        setIndex(importItems.bulkExport(tenantId));
    }
}
