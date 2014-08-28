package com.jivesoftware.os.miru.service.index.memory;

import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.query.MiruBitmaps;
import com.jivesoftware.os.miru.query.MiruInvertedIndex;
import com.jivesoftware.os.miru.query.ReusableBuffers;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.BulkImport;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/** @author jonathan */
public class MiruInMemoryInvertedIndex<BM> implements MiruInvertedIndex<BM>, BulkImport<BM>, BulkExport<BM> {

    private final MiruBitmaps<BM> bitmaps;
    private final ReusableBuffers<BM> reusable;
    private final AtomicReference<BM> read;
    private final AtomicReference<BM> write;
    private volatile boolean needsMerge;

    public MiruInMemoryInvertedIndex(MiruBitmaps<BM> bitmaps) {
        this.bitmaps = bitmaps;
        this.reusable = new ReusableBuffers<>(bitmaps, 3); // screw you not exposing to config!
        this.read = new AtomicReference<>(bitmaps.create());
        this.write = new AtomicReference<>();
        this.needsMerge = false;
    }

    @Override
    public BM getIndex() throws Exception {
        if (needsMerge) {
            synchronized (write) {
                merge();
            }
        }
        return read.get();
    }

    private void markForMerge() {
        needsMerge = true;
    }

    /* Synchronize externally */
    private void merge() {
        BM writer = write.get();
        if (writer != null) {
            reusable.retain(writer, read.get());
            read.set(writer);
            write.set(null);
        }
        // flip the flag last, since we don't want getIndex() calls to slip through before the merge is finished
        needsMerge = false;
    }

    /* Synchronize externally */
    private BM writer() {
        BM bitmap = write.get();
        if (bitmap == null) {
            bitmap = copy(read.get());
            write.set(bitmap);
        }
        return bitmap;
    }

    @Override
    public void append(int id) {
        synchronized (write) {
            BM bitmap = writer();
            if (!bitmaps.set(bitmap, id)) {
                throw new RuntimeException("id must be in increasing order"
                    + ", id = " + id
                    + ", cardinality = " + bitmaps.cardinality(bitmap)
                    + ", size in bits = " + bitmaps.sizeInBits(bitmap));
            }
            markForMerge();
        }
    }

    @Override
    public void appendAndExtend(List<Integer> ids, int lastId) throws Exception {
        synchronized (write) {
            BM bitmap = writer();
            bitmaps.extend(bitmap, ids, lastId + 1);
            markForMerge();
        }
    }

    @Override
    public void remove(int id) { // Kinda crazy expensive way to remove an intermediary bit.
        synchronized (write) {
            BM remove = reusable.next();
            bitmaps.set(remove, id);
            BM bitmap = writer();
            BM r = bitmaps.create();
            bitmaps.andNotToSourceSize(r, bitmap, remove);
            write.set(r);
            markForMerge();
        }
    }

    @Override
    public void set(int id) { // Kinda crazy expensive way to set an intermediary bit.
        synchronized (write) {
            BM set = reusable.next();
            bitmaps.set(set, id);
            BM bitmap = writer();
            BM r = reusable.next();
            bitmaps.or(r, Arrays.asList(bitmap, set));
            write.set(r);
            markForMerge();
        }
    }

    @Override
    public void setIntermediate(int... ids) {
        synchronized (write) {
            BM bitmap = writer();
            BM r = bitmaps.setIntermediate(bitmap, ids);
            write.set(r);
            markForMerge();
        }
    }

    @Override
    public void andNot(BM mask) {
        if (bitmaps.sizeInBits(mask) == 0) {
            return;
        }
        synchronized (write) {
            BM bitmap = writer();
            BM r = reusable.next();
            bitmaps.andNot(r, bitmap, Collections.singletonList(mask));
            write.set(r);
            markForMerge();
        }
    }

    @Override
    public void or(BM mask) {
        if (bitmaps.sizeInBits(mask) == 0) {
            return;
        }
        synchronized (write) {
            BM bitmap = writer();
            BM r = reusable.next();
            bitmaps.or(r, Arrays.asList(bitmap, mask));
            write.set(r);
            markForMerge();
        }
    }

    @Override
    public void andNotToSourceSize(BM mask) {
        if (bitmaps.sizeInBits(mask) == 0) {
            return;
        }
        synchronized (write) {
            BM bitmap = writer();
            BM andNot = bitmaps.create();
            bitmaps.andNotToSourceSize(andNot, bitmap, mask);
            write.set(andNot);
            markForMerge();
        }
    }

    @Override
    public void orToSourceSize(BM mask) {
        if (bitmaps.sizeInBits(mask) == 0) {
            return;
        }
        synchronized (write) {
            BM bitmap = writer();
            BM or = bitmaps.create();
            bitmaps.orToSourceSize(or, bitmap, mask);
            write.set(or);
            markForMerge();
        }
    }

    @Override
    public long sizeInMemory() {
        long sizeInBytes = bitmaps.sizeInBytes(read.get());
        // deliberately not getting a lock, should be safe just to peek at the size
        BM writer = write.get();
        if (writer != null) {
            sizeInBytes += bitmaps.sizeInBytes(writer);
        }
        return sizeInBytes;
    }

    @Override
    public long sizeOnDisk() throws Exception {
        return 0;
    }

    @Override
    public BM bulkExport(MiruTenantId tenantId) throws Exception {
        synchronized (write) {
            merge();
            return read.get();
        }
    }

    @Override
    public void bulkImport(MiruTenantId tenantId, BulkExport<BM> importItems) throws Exception {
        synchronized (write) {
            write.set(importItems.bulkExport(tenantId));
            merge();
        }
    }

    private BM copy(BM original) {
        //TODO fix BM.clone()
        if (bitmaps.sizeInBits(original) == 0) {
            return reusable.next();
        } else {
            BM next = reusable.next();
            bitmaps.copy(next, original);
            return next;
        }
    }

}
