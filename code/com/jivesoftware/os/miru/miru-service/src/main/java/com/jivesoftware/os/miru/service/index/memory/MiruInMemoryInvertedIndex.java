package com.jivesoftware.os.miru.service.index.memory;

import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.BulkImport;
import com.jivesoftware.os.miru.service.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.service.stream.filter.MatchNoMoreThanNBitmapStorage;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** @author jonathan */
public class MiruInMemoryInvertedIndex implements MiruInvertedIndex, BulkImport<EWAHCompressedBitmap>, BulkExport<EWAHCompressedBitmap> {

    private final AtomicReference<EWAHCompressedBitmap> read;
    private final AtomicReference<EWAHCompressedBitmap> write;
    private final AtomicBoolean needsMerge;

    public MiruInMemoryInvertedIndex(EWAHCompressedBitmap invertedIndex) {
        this.read = new AtomicReference<>(invertedIndex);
        this.write = new AtomicReference<>();
        this.needsMerge = new AtomicBoolean();
    }

    @Override
    public EWAHCompressedBitmap getIndex() throws Exception {
        if (needsMerge.get()) {
            synchronized (write) {
                merge();
            }
        }
        return read.get();
    }

    private void markForMerge() {
        needsMerge.set(true);
    }

    /* Synchronize externally */
    private void merge() {
        EWAHCompressedBitmap writer = write.get();
        if (writer != null) {
            read.set(writer);
            write.set(null);
        }
        // flip the flag last, since we don't want getIndex() calls to slip through before the merge is finished
        needsMerge.set(false);
    }

    /* Synchronize externally */
    private EWAHCompressedBitmap writer() {
        EWAHCompressedBitmap bitmap = write.get();
        if (bitmap == null) {
            bitmap = copy(read.get());
            write.set(bitmap);
        }
        return bitmap;
    }

    @Override
    public void append(int id) {
        synchronized (write) {
            EWAHCompressedBitmap bitmap = writer();
            if (!bitmap.set(id)) {
                throw new RuntimeException("id must be in increasing order"
                    + ", id = " + id
                    + ", cardinality = " + bitmap.cardinality()
                    + ", size in bits = " + bitmap.sizeInBits());
            }
            markForMerge();
        }
    }

    @Override
    public void appendAndExtend(List<Integer> ids, int lastId) throws Exception {
        synchronized (write) {
            EWAHCompressedBitmap bitmap = writer();
            if (ids.isEmpty() && bitmap.sizeInBits() == lastId + 1) {
                return;
            }
            for (int id : ids) {
                if (!bitmap.set(id)) {
                    throw new RuntimeException("id must be in increasing order"
                        + ", id = " + id
                        + ", cardinality = " + bitmap.cardinality()
                        + ", size in bits = " + bitmap.sizeInBits());
                }
            }
            bitmap.setSizeInBits(lastId + 1, false);
            markForMerge();
        }
    }

    @Override
    public void remove(int id) { // Kinda crazy expensive way to remove an intermediary bit.
        EWAHCompressedBitmap remove = new EWAHCompressedBitmap();
        remove.set(id);
        synchronized (write) {
            EWAHCompressedBitmap bitmap = writer();
            EWAHCompressedBitmap r = MiruInMemoryInvertedIndex.this.andNotToSourceSize(bitmap, remove);
            write.set(r);
            markForMerge();
        }
    }

    @Override
    public void set(int id) { // Kinda crazy expensive way to set an intermediary bit.
        EWAHCompressedBitmap set = new EWAHCompressedBitmap();
        set.set(id);
        synchronized (write) {
            EWAHCompressedBitmap bitmap = writer();
            EWAHCompressedBitmap r = bitmap.or(set);
            write.set(r);
            markForMerge();
        }
    }

    @Override
    public void andNot(EWAHCompressedBitmap mask) {
        if (mask.sizeInBits() == 0) {
            return;
        }
        synchronized (write) {
            EWAHCompressedBitmap bitmap = writer();
            write.set(bitmap.andNot(mask));
            markForMerge();
        }
    }

    @Override
    public void or(EWAHCompressedBitmap mask) {
        if (mask.sizeInBits() == 0) {
            return;
        }
        synchronized (write) {
            EWAHCompressedBitmap bitmap = writer();
            write.set(bitmap.or(mask));
            markForMerge();
        }
    }

    @Override
    public void andNotToSourceSize(EWAHCompressedBitmap mask) {
        if (mask.sizeInBits() == 0) {
            return;
        }
        synchronized (write) {
            EWAHCompressedBitmap bitmap = writer();
            EWAHCompressedBitmap andNot = andNotToSourceSize(bitmap, mask);
            write.set(andNot);
            markForMerge();
        }
    }

    @Override
    public void orToSourceSize(EWAHCompressedBitmap mask) {
        if (mask.sizeInBits() == 0) {
            return;
        }
        synchronized (write) {
            EWAHCompressedBitmap bitmap = writer();
            EWAHCompressedBitmap or = orToSourceSize(bitmap, mask);
            write.set(or);
            markForMerge();
        }
    }

    @Override
    public long sizeInMemory() {
        long sizeInBytes = read.get().sizeInBytes();
        // deliberately not getting a lock, should be safe just to peek at the size
        EWAHCompressedBitmap writer = write.get();
        if (writer != null) {
            sizeInBytes += writer.sizeInBytes();
        }
        return sizeInBytes;
    }

    @Override
    public long sizeOnDisk() throws Exception {
        return 0;
    }

    @Override
    public EWAHCompressedBitmap bulkExport() throws Exception {
        synchronized (write) {
            merge();
            return read.get();
        }
    }

    @Override
    public void bulkImport(BulkExport<EWAHCompressedBitmap> importItems) throws Exception {
        synchronized (write) {
            write.set(importItems.bulkExport());
            merge();
        }
    }

    private EWAHCompressedBitmap copy(EWAHCompressedBitmap original) {
        //TODO fix EWAHCompressedBitmap.clone()
        if (original.sizeInBits() == 0) {
            return new EWAHCompressedBitmap();
        } else {
            return original.or(new EWAHCompressedBitmap());
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
}
