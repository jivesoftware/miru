package com.jivesoftware.os.miru.service.index.memory;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.jivesoftware.os.jive.utils.io.FilerIO;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.query.MiruBitmaps;
import com.jivesoftware.os.miru.query.MiruIndex;
import com.jivesoftware.os.miru.query.MiruInvertedIndex;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.BulkImport;
import gnu.trove.impl.Constants;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.procedure.TLongObjectProcedure;
import java.util.Map;

/**
 * @author jonathan
 */
public class MiruInMemoryIndex<BM> implements MiruIndex<BM>, BulkImport<Map<Long, MiruInvertedIndex<BM>>>, BulkExport<Map<Long, MiruInvertedIndex<BM>>> {

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
        long fieldTermId = FilerIO.bytesLong(FilerIO.intArrayToByteArray(new int[]{fieldId, termId}));
        synchronized (index) {
            return Optional.fromNullable(index.get(fieldTermId));
        }
    }

    private MiruInvertedIndex<BM> getOrAllocate(int fieldId, int termId) {
        Optional<MiruInvertedIndex<BM>> got = get(fieldId, termId);
        if (!got.isPresent()) {
            long fieldTermId = FilerIO.bytesLong(FilerIO.intArrayToByteArray(new int[]{fieldId, termId}));
            MiruInMemoryInvertedIndex<BM> miruInvertedIndex = new MiruInMemoryInvertedIndex<>(bitmaps);

            got = Optional.<MiruInvertedIndex<BM>>of(miruInvertedIndex);
            synchronized (index) {
                index.put(fieldTermId, miruInvertedIndex);
            }
        }
        return got.get();
    }

    @Override
    public Map<Long, MiruInvertedIndex<BM>> bulkExport(MiruTenantId tenantId) throws Exception {
        synchronized (index) {
            final ImmutableMap.Builder<Long, MiruInvertedIndex<BM>> builder = ImmutableMap.builder();
            index.forEachEntry(new TLongObjectProcedure<MiruInvertedIndex<BM>>() {
                @Override
                public boolean execute(long a, MiruInvertedIndex<BM> b) {
                    builder.put(a, b);
                    return true;
                }
            });
            return builder.build();
        }
    }

    @Override
    public void bulkImport(MiruTenantId tenantId, BulkExport<Map<Long, MiruInvertedIndex<BM>>> importItems) throws Exception {
        synchronized (index) {
            index.putAll(importItems.bulkExport(tenantId));
        }
    }
}
