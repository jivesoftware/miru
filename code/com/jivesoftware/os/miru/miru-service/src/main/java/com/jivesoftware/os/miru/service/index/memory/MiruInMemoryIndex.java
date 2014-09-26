package com.jivesoftware.os.miru.service.index.memory;

import com.google.common.base.Optional;
import com.jivesoftware.os.jive.utils.io.FilerIO;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.service.index.BulkEntry;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.BulkImport;
import gnu.trove.impl.Constants;
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.hash.TLongObjectHashMap;
import java.util.Iterator;

/**
 * @author jonathan
 */
public class MiruInMemoryIndex<BM> implements MiruIndex<BM>, BulkImport<Iterator<BulkEntry<Long, MiruInvertedIndex<BM>>>>,
    BulkExport<Iterator<BulkEntry<Long, MiruInvertedIndex<BM>>>> {

    private final MiruBitmaps<BM> bitmaps;
    private final TLongObjectHashMap<MiruInvertedIndex<BM>> index;

    public MiruInMemoryIndex(MiruBitmaps<BM> bitmaps) {
        this.bitmaps = bitmaps;
        this.index = new TLongObjectHashMap<>(Constants.DEFAULT_CAPACITY, Constants.DEFAULT_LOAD_FACTOR, -1);
    }

    @Override
    public long sizeInMemory() throws Exception {
        long sizeInBytes = 0;
        synchronized (index) {
            for (MiruInvertedIndex<BM> i : index.valueCollection()) {
                sizeInBytes += bitmaps.sizeInBytes(i.getIndex());
            }
        }
        return sizeInBytes;
    }

    @Override
    public long sizeOnDisk() throws Exception {
        return 0;
    }

    @Override
    public MiruInvertedIndex<BM> allocate(int fieldId, int termId) throws Exception {
        return getOrAllocate(fieldId, termId);
    }

    @Override
    public void index(int fieldId, int termId, int id) throws Exception {
        getOrAllocate(fieldId, termId).append(id);
    }

    @Override
    public void remove(int fieldId, int termId, int id) throws Exception {
        Optional<MiruInvertedIndex<BM>> got = get(fieldId, termId);
        if (got.isPresent()) {
            got.get().remove(id);
        }
    }

    @Override
    public Optional<MiruInvertedIndex<BM>> get(int fieldId, int termId) {
        long fieldTermId = FilerIO.bytesLong(FilerIO.intArrayToByteArray(new int[] { fieldId, termId }));
        synchronized (index) {
            return Optional.fromNullable(index.get(fieldTermId));
        }
    }

    private MiruInvertedIndex<BM> getOrAllocate(int fieldId, int termId) {
        Optional<MiruInvertedIndex<BM>> got = get(fieldId, termId);
        if (!got.isPresent()) {
            long fieldTermId = FilerIO.bytesLong(FilerIO.intArrayToByteArray(new int[] { fieldId, termId }));
            MiruInMemoryInvertedIndex<BM> miruInvertedIndex = new MiruInMemoryInvertedIndex<>(bitmaps);

            got = Optional.<MiruInvertedIndex<BM>>of(miruInvertedIndex);
            synchronized (index) {
                index.put(fieldTermId, miruInvertedIndex);
            }
        }
        return got.get();
    }

    @Override
    public Iterator<BulkEntry<Long, MiruInvertedIndex<BM>>> bulkExport(MiruTenantId tenantId) throws Exception {
        synchronized (index) {
            final TLongObjectIterator<MiruInvertedIndex<BM>> iter = index.iterator();
            return new Iterator<BulkEntry<Long, MiruInvertedIndex<BM>>>() {

                @Override
                public boolean hasNext() {
                    return iter.hasNext();
                }

                @Override
                public BulkEntry<Long, MiruInvertedIndex<BM>> next() {
                    iter.advance();
                    return new BulkEntry<>(iter.key(), iter.value());
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }
    }

    @Override
    public void bulkImport(MiruTenantId tenantId, BulkExport<Iterator<BulkEntry<Long, MiruInvertedIndex<BM>>>> importItems) throws Exception {
        synchronized (index) {
            Iterator<BulkEntry<Long, MiruInvertedIndex<BM>>> iter = importItems.bulkExport(tenantId);
            while (iter.hasNext()) {
                BulkEntry<Long, MiruInvertedIndex<BM>> entry = iter.next();
                index.put(entry.key, entry.value);
            }
        }
    }
}
