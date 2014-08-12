package com.jivesoftware.os.miru.service.index.memory;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.BulkImport;
import com.jivesoftware.os.miru.service.index.MiruIndex;
import com.jivesoftware.os.miru.service.index.MiruInvertedIndex;
import com.jivesoftware.os.jive.utils.io.FilerIO;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author jonathan
 */
public class MiruInMemoryIndex implements MiruIndex, BulkImport<Map<Long, MiruInvertedIndex>>, BulkExport<Map<Long, MiruInvertedIndex>> {

    private final ConcurrentMap<Long, MiruInvertedIndex> index;

    public MiruInMemoryIndex() {
        this.index = new ConcurrentHashMap<>();
    }

    @Override
    public long sizeInMemory() throws Exception {
        long sizeInBytes = 0;
        for (MiruInvertedIndex i : index.values()) {
            sizeInBytes += i.getIndex().sizeInBytes();
        }
        return sizeInBytes;
    }

    @Override
    public long sizeOnDisk() throws Exception {
        return 0;
    }

    @Override
    public void index(int fieldId, int termId, int id) throws Exception {
        getOrAllocate(fieldId, termId).append(id);
    }

    @Override
    public void remove(int fieldId, int termId, int id) throws Exception {
        Optional<MiruInvertedIndex> got = get(fieldId, termId);
        if (got.isPresent()) {
            got.get().remove(id);
        }
    }

    @Override
    public Optional<MiruInvertedIndex> get(int fieldId, int termId) {
        long fieldTermId = FilerIO.bytesLong(FilerIO.intArrayToByteArray(new int[]{fieldId, termId}));
        return Optional.<MiruInvertedIndex>fromNullable(index.get(fieldTermId));
    }

    private MiruInvertedIndex getOrAllocate(int fieldId, int termId) {
        Optional<MiruInvertedIndex> got = get(fieldId, termId);
        if (!got.isPresent()) {
            long fieldTermId = FilerIO.bytesLong(FilerIO.intArrayToByteArray(new int[]{fieldId, termId}));
            MiruInMemoryInvertedIndex miruInvertedIndex = new MiruInMemoryInvertedIndex(new EWAHCompressedBitmap());

            got = Optional.<MiruInvertedIndex>of(miruInvertedIndex);
            index.put(fieldTermId, miruInvertedIndex);
        }
        return got.get();
    }

    @Override
    public Map<Long, MiruInvertedIndex> bulkExport() throws Exception {
        return ImmutableMap.copyOf(index);
    }

    @Override
    public void bulkImport(BulkExport<Map<Long, MiruInvertedIndex>> importItems) throws Exception {
        index.putAll(importItems.bulkExport());
    }
}
