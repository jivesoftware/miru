package com.jivesoftware.os.miru.service.index.disk;

import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.BulkImport;
import com.jivesoftware.os.miru.service.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.service.stream.filter.MatchNoMoreThanNBitmapStorage;
import com.jivesoftware.os.jive.utils.io.FilerIO;
import com.jivesoftware.os.jive.utils.keyed.store.SwappableFiler;
import com.jivesoftware.os.jive.utils.keyed.store.SwappingFiler;
import java.io.DataInput;
import java.util.List;

/** @author jonathan */
public class MiruOnDiskInvertedIndex implements MiruInvertedIndex, BulkImport<EWAHCompressedBitmap> {

    private final SwappableFiler filer;
    private final int startPosition;

    public MiruOnDiskInvertedIndex(SwappableFiler filer) {
        this(filer, 0);
    }

    public MiruOnDiskInvertedIndex(SwappableFiler filer, int startPosition) {
        this.filer = filer;
        this.startPosition = startPosition;
    }

    @Override
    public EWAHCompressedBitmap getIndex() throws Exception {
        EWAHCompressedBitmap index = new EWAHCompressedBitmap();
        synchronized (filer.lock()) {
            filer.sync();
            filer.seek(startPosition);
            DataInput dataInput = FilerIO.asDataInput(filer);
            if (dataInput.readInt() > 0) {
                filer.seek(startPosition);
                index.deserialize(dataInput);
            }
        }
        return index;
    }

    @Override
    public void append(int id) throws Exception {
        synchronized (filer.lock()) {
            filer.sync();
            EWAHCompressedBitmap index = getIndex();
            if (!index.set(id)) {
                throw new RuntimeException("id must be in increasing order"
                    + ", id = " + id
                    + ", cardinality = " + index.cardinality()
                    + ", size in bits = " + index.sizeInBits());
            }
            setIndex(index);
        }
    }

    @Override
    public void appendAndExtend(List<Integer> ids, int lastId) throws Exception {
        synchronized (filer.lock()) {
            filer.sync();
            EWAHCompressedBitmap index = getIndex();
            if (ids.isEmpty() && index.sizeInBits() == lastId + 1) {
                return;
            }
            for (int id : ids) {
                if (!index.set(id)) {
                    throw new RuntimeException("id must be in increasing order"
                        + ", id = " + id
                        + ", cardinality = " + index.cardinality()
                        + ", size in bits = " + index.sizeInBits());
                }
            }
            index.setSizeInBits(lastId + 1, false);
            setIndex(index);
        }
    }

    @Override
    public void remove(int id) throws Exception { // Kinda crazy expensive way to remove an intermediary bit.
        EWAHCompressedBitmap remove = new EWAHCompressedBitmap();
        remove.set(id);
        synchronized (filer.lock()) {
            filer.sync();
            EWAHCompressedBitmap r = andNotToSourceSize(getIndex(), remove);
            setIndex(r);
        }
    }

    @Override
    public void set(int id) throws Exception { // Kinda crazy expensive way to set an intermediary bit.
        EWAHCompressedBitmap set = new EWAHCompressedBitmap();
        set.set(id);
        synchronized (filer.lock()) {
            filer.sync();
            EWAHCompressedBitmap r = getIndex().or(set);
            setIndex(r);
        }
    }

    @Override
    public void andNot(EWAHCompressedBitmap mask) throws Exception {
        synchronized (filer.lock()) {
            filer.sync();
            EWAHCompressedBitmap andNot = andNotToSourceSize(getIndex(), mask);
            setIndex(andNot);
        }
    }

    @Override
    public void or(EWAHCompressedBitmap mask) throws Exception {
        synchronized (filer.lock()) {
            filer.sync();
            EWAHCompressedBitmap or = orToSourceSize(getIndex(), mask);
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

    private void setIndex(EWAHCompressedBitmap index) throws Exception {
        synchronized (filer.lock()) {
            filer.sync();
            filer.seek(0);
            byte[] initialBytes = new byte[startPosition];
            FilerIO.read(filer, initialBytes);

            SwappingFiler swap = filer.swap(startPosition + index.serializedSizeInBytes());
            FilerIO.write(swap, initialBytes);
            index.serialize(FilerIO.asDataOutput(swap));
            swap.commit();
        }
    }

    private EWAHCompressedBitmap orToSourceSize(EWAHCompressedBitmap source, EWAHCompressedBitmap mask) {
        EWAHCompressedBitmap result = new EWAHCompressedBitmap();
        MatchNoMoreThanNBitmapStorage matchNoMoreThanNBitmapStorage = new MatchNoMoreThanNBitmapStorage(result, source.sizeInBits());
        source.orToContainer(mask, matchNoMoreThanNBitmapStorage);
        return result;
    }

    private EWAHCompressedBitmap andNotToSourceSize(EWAHCompressedBitmap source, EWAHCompressedBitmap mask) {
        EWAHCompressedBitmap result = new EWAHCompressedBitmap();
        MatchNoMoreThanNBitmapStorage matchNoMoreThanNBitmapStorage = new MatchNoMoreThanNBitmapStorage(result, source.sizeInBits());
        source.andNotToContainer(mask, matchNoMoreThanNBitmapStorage);
        return result;
    }

    @Override
    public void bulkImport(BulkExport<EWAHCompressedBitmap> importItems) throws Exception {
        setIndex(importItems.bulkExport());
    }
}
