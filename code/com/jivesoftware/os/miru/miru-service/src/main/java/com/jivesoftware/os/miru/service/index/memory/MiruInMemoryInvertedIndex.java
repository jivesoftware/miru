package com.jivesoftware.os.miru.service.index.memory;

import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.ReusableBuffers;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.service.index.BitmapAndLastId;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.BulkImport;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/** @author jonathan */
public class MiruInMemoryInvertedIndex<BM> implements MiruInvertedIndex<BM>, BulkImport<BitmapAndLastId<BM>>, BulkExport<BitmapAndLastId<BM>> {

    private final MiruBitmaps<BM> bitmaps;
    private final ReusableBuffers<BM> reusable;
    private final AtomicReference<BM> read;
    private final AtomicReference<BM> write;
    private volatile int lastId = -1;
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

    @Override
    public BM getIndexUnsafe() throws Exception {
        BM index = write.get();
        if (index == null) {
            index = read.get();
        }
        return index;
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
    public void append(int... ids) {
        if (ids.length == 0) {
            return;
        }
        synchronized (write) {
            BM bitmap = writer();
            if (!bitmaps.set(bitmap, ids)) {
                throw new RuntimeException("id must be in increasing order"
                    + ", ids = " + Arrays.toString(ids)
                    + ", cardinality = " + bitmaps.cardinality(bitmap)
                    + ", size in bits = " + bitmaps.sizeInBits(bitmap));
            }
            markForMerge();

            int appendLastId = ids[ids.length - 1];
            if (appendLastId > lastId) {
                lastId = appendLastId;
            }
        }
    }

    @Override
    public void appendAndExtend(List<Integer> ids, int extendToId) throws Exception {
        synchronized (write) {
            BM bitmap = writer();
            bitmaps.extend(bitmap, ids, extendToId + 1);
            markForMerge();

            if (!ids.isEmpty()) {
                int appendLastId = ids.get(ids.size() - 1);
                if (appendLastId > lastId) {
                    lastId = appendLastId;
                }
            }
        }
    }

    @Override
    public void remove(int id) {
        BM r = bitmaps.create();
        synchronized (write) {
            BM remove = reusable.next();
            bitmaps.set(remove, id);
            BM bitmap = writer();
            bitmaps.andNotToSourceSize(r, bitmap, Collections.singletonList(remove));
            write.set(r);
            markForMerge();
        }
    }

    @Override
    public void set(int... ids) {
        if (ids.length == 0) {
            return;
        }
        synchronized (write) {
            BM set = reusable.next();
            bitmaps.set(set, ids);
            BM bitmap = writer();
            BM r = reusable.next();
            bitmaps.or(r, Arrays.asList(bitmap, set));
            write.set(r);
            markForMerge();

            int setLastId = ids[ids.length - 1];
            if (setLastId > lastId) {
                lastId = setLastId;
            }
        }
    }

    @Override
    public void setIntermediate(int... ids) {
        if (ids.length == 0) {
            return;
        }
        synchronized (write) {
            BM bitmap = writer();
            BM r = bitmaps.setIntermediate(bitmap, ids);
            write.set(r);
            markForMerge();

            int setLastId = ids[ids.length - 1];
            if (setLastId > lastId) {
                lastId = setLastId;
            }
        }
    }

    @Override
    public int lastId() {
        return lastId;
    }

    @Override
    public void andNot(BM mask) {
        if (bitmaps.isEmpty(mask)) {
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
        if (bitmaps.isEmpty(mask)) {
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
    public void andNotToSourceSize(List<BM> masks) {
        if (masks.isEmpty()) {
            return;
        }
        for (BM mask : masks) {
            if (bitmaps.isEmpty(mask)) {
                return;
            }
        }
        BM andNot = bitmaps.create();
        synchronized (write) {
            BM bitmap = writer();
            bitmaps.andNotToSourceSize(andNot, bitmap, masks);
            write.set(andNot);
            markForMerge();
        }
    }

    @Override
    public void orToSourceSize(BM mask) {
        if (bitmaps.isEmpty(mask)) {
            return;
        }
        BM or = bitmaps.create();
        synchronized (write) {
            BM bitmap = writer();
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
    public BitmapAndLastId<BM> bulkExport(MiruTenantId tenantId) throws Exception {
        synchronized (write) {
            merge();
            return new BitmapAndLastId<>(read.get(), lastId);
        }
    }

    @Override
    public void bulkImport(MiruTenantId tenantId, BulkExport<BitmapAndLastId<BM>> importItems) throws Exception {
        synchronized (write) {
            BitmapAndLastId<BM> bitmapAndLastId = importItems.bulkExport(tenantId);
            write.set(bitmapAndLastId.bitmap);
            merge();
            lastId = bitmapAndLastId.lastId;
        }
    }

    private BM copy(BM original) {
        //TODO fix BM.clone()
        if (bitmaps.isEmpty(original)) {
            return reusable.next();
        } else {
            BM next = reusable.next();
            bitmaps.copy(next, original);
            return next;
        }
    }

}
