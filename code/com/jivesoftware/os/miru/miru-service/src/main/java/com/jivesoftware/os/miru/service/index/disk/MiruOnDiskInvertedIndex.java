package com.jivesoftware.os.miru.service.index.disk;

import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.keyed.store.SwappableFiler;
import com.jivesoftware.os.filer.keyed.store.SwappingFiler;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.service.index.BitmapAndLastId;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.BulkImport;
import java.io.DataInput;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** @author jonathan */
public class MiruOnDiskInvertedIndex<BM> implements MiruInvertedIndex<BM>, BulkImport<BitmapAndLastId<BM>>, BulkExport<BitmapAndLastId<BM>> {

    private static final int LAST_ID_LENGTH = 4;

    private final MiruBitmaps<BM> bitmaps;
    private final SwappableFiler filer;
    private final int startPosition;
    private final Object mutationLock = new Object();
    private volatile int lastId = Integer.MIN_VALUE;

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
            filer.seek(0);
            byte[] bytes = new byte[(int) filer.length()];
            FilerIO.read(filer, bytes);
            //TODO just add a byte marker, this sucks
            //TODO also, get rid of the startPosition crap
            if (bytes.length > startPosition + LAST_ID_LENGTH + 4 && FilerIO.bytesInt(bytes, startPosition + LAST_ID_LENGTH) > 0) {
                DataInput dataInput = ByteStreams.newDataInput(bytes, startPosition + LAST_ID_LENGTH);
                return bitmaps.deserialize(dataInput);
            }
        }
        return bitmaps.create();
    }

    private void setIndex(BM index, int setLastId) throws Exception {
        long bitmapSizeInBytes = bitmaps.serializedSizeInBytes(index);
        ByteArrayDataOutput dataOutput = ByteStreams.newDataOutput(startPosition + LAST_ID_LENGTH + (int) bitmapSizeInBytes);
        dataOutput.write(new byte[startPosition]);
        dataOutput.write(FilerIO.intBytes(setLastId));
        bitmaps.serialize(index, dataOutput);

        synchronized (filer.lock()) {
            filer.sync();
            filer.seek(0);
            byte[] bytes = dataOutput.toByteArray();
            FilerIO.read(filer, bytes, 0, startPosition);

            SwappingFiler swap = filer.swap(startPosition + LAST_ID_LENGTH + bitmapSizeInBytes);
            swap.write(bytes);
            swap.commit();
        }
    }

    @Override
    public BM getIndexUnsafe() throws Exception {
        return getIndex();
    }

    @Override
    public void append(int... ids) throws Exception {
        if (ids.length == 0) {
            return;
        }
        synchronized (mutationLock) {
            BM index = getIndex();
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
            BM index = getIndex();
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
            BM r = bitmaps.create();
            bitmaps.andNot(r, getIndex(), Collections.singletonList(remove));
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
            BM r = bitmaps.create();
            bitmaps.or(r, Arrays.asList(getIndex(), set));

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
            BM r = bitmaps.setIntermediate(getIndex(), ids);

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
            synchronized (filer.lock()) {
                filer.sync();
                filer.seek(startPosition);
                lastId = FilerIO.readInt(filer, "lastId");
            }
        }
        return lastId;
    }

    @Override
    public void andNot(BM mask) throws Exception {
        synchronized (mutationLock) {
            BM r = bitmaps.create();
            bitmaps.andNot(r, getIndex(), Collections.singletonList(mask));
            setIndex(r, lastId);
        }
    }

    @Override
    public void or(BM mask) throws Exception {
        synchronized (mutationLock) {
            BM r = bitmaps.create();
            bitmaps.or(r, Arrays.asList(getIndex(), mask));
            setIndex(r, lastId);
        }
    }

    @Override
    public void andNotToSourceSize(List<BM> masks) throws Exception {
        synchronized (mutationLock) {
            BM andNot = bitmaps.create();
            bitmaps.andNotToSourceSize(andNot, getIndex(), masks);
            setIndex(andNot, lastId);
        }
    }

    @Override
    public void orToSourceSize(BM mask) throws Exception {
        synchronized (mutationLock) {
            BM or = bitmaps.create();
            bitmaps.orToSourceSize(or, getIndex(), mask);
            setIndex(or, lastId);
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

    @Override
    public void bulkImport(MiruTenantId tenantId, BulkExport<BitmapAndLastId<BM>> importItems) throws Exception {
        BitmapAndLastId<BM> bitmapAndLastId = importItems.bulkExport(tenantId);
        setIndex(bitmapAndLastId.bitmap, bitmapAndLastId.lastId);
    }

    @Override
    public BitmapAndLastId<BM> bulkExport(MiruTenantId tenantId) throws Exception {
        return new BitmapAndLastId<>(getIndexUnsafe(), lastId);
    }
}
