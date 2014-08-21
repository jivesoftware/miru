package com.jivesoftware.os.miru.service.index.memory;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.jivesoftware.os.jive.utils.io.FilerIO;
import com.jivesoftware.os.miru.service.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.BulkImport;
import com.jivesoftware.os.miru.service.index.MiruIndex;
import com.jivesoftware.os.miru.service.index.MiruInvertedIndex;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author jonathan
 */
public class MiruInMemoryIndex<BM> implements MiruIndex<BM>, BulkImport<Map<Long, MiruInvertedIndex<BM>>>, BulkExport<Map<Long, MiruInvertedIndex<BM>>> {

    private final MiruBitmaps<BM> bitmaps;
    private final ConcurrentMap<Long, MiruInvertedIndex<BM>> index;

    public MiruInMemoryIndex(MiruBitmaps<BM> bitmaps) {
        this.bitmaps = bitmaps;
        this.index = new ConcurrentHashMap<>();
    }

    @Override
    public long sizeInMemory() throws Exception {
        long sizeInBytes = 0;
        for (MiruInvertedIndex<BM> i : index.values()) {
            sizeInBytes += bitmaps.sizeInBytes(i.getIndex());
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
        long fieldTermId = FilerIO.bytesLong(FilerIO.intArrayToByteArray(new int[]{fieldId, termId}));
        return Optional.<MiruInvertedIndex<BM>>fromNullable(index.get(fieldTermId));
    }

    private MiruInvertedIndex<BM> getOrAllocate(int fieldId, int termId) {
        Optional<MiruInvertedIndex<BM>> got = get(fieldId, termId);
        if (!got.isPresent()) {
            long fieldTermId = FilerIO.bytesLong(FilerIO.intArrayToByteArray(new int[]{fieldId, termId}));
            MiruInMemoryInvertedIndex<BM> miruInvertedIndex = new MiruInMemoryInvertedIndex<>(bitmaps);

            got = Optional.<MiruInvertedIndex<BM>>of(miruInvertedIndex);
            index.put(fieldTermId, miruInvertedIndex);
        }
        return got.get();
    }

    @Override
    public Map<Long, MiruInvertedIndex<BM>> bulkExport() throws Exception {
        return ImmutableMap.copyOf(index);
    }

    @Override
    public void bulkImport(BulkExport<Map<Long, MiruInvertedIndex<BM>>> importItems) throws Exception {
        index.putAll(importItems.bulkExport());
    }
}
