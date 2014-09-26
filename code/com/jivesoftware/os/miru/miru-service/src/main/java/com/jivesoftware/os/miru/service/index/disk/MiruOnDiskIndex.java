package com.jivesoftware.os.miru.service.index.disk;

import com.google.common.base.Optional;
import com.jivesoftware.os.jive.utils.chunk.store.MultiChunkStore;
import com.jivesoftware.os.jive.utils.io.FilerIO;
import com.jivesoftware.os.jive.utils.keyed.store.FileBackedKeyedStore;
import com.jivesoftware.os.jive.utils.keyed.store.SwappableFiler;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.service.index.BulkEntry;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.BulkImport;
import java.io.File;
import java.util.Iterator;

/** @author jonathan */
public class MiruOnDiskIndex<BM> implements MiruIndex<BM>, BulkImport<Iterator<BulkEntry<Long, MiruInvertedIndex<BM>>>> {

    private final MiruBitmaps<BM> bitmaps;
    private final FileBackedKeyedStore index;

    public MiruOnDiskIndex(MiruBitmaps<BM> bitmaps, File mapDirectory, File swapDirectory, MultiChunkStore chunkStore) throws Exception {
        this.bitmaps = bitmaps;
        this.index = new FileBackedKeyedStore(mapDirectory.getAbsolutePath(), swapDirectory.getAbsolutePath(), 8, 100, chunkStore, 512);
    }

    @Override
    public long sizeInMemory() throws Exception {
        return 0;
    }

    @Override
    public long sizeOnDisk() throws Exception {
        return index.mapStoreSizeInBytes();
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
    public Optional<MiruInvertedIndex<BM>> get(int fieldId, int termId) throws Exception {
        long fieldTermId = FilerIO.bytesLong(FilerIO.intArrayToByteArray(new int[] { fieldId, termId }));
        SwappableFiler filer = index.get(FilerIO.longBytes(fieldTermId), false);
        if (filer == null) {
            return Optional.absent();
        }
        return Optional.<MiruInvertedIndex<BM>>of(new MiruOnDiskInvertedIndex<>(bitmaps, filer));
    }

    private MiruInvertedIndex<BM> getOrAllocate(int fieldId, int termId) throws Exception {
        long fieldTermId = FilerIO.bytesLong(FilerIO.intArrayToByteArray(new int[] { fieldId, termId }));
        SwappableFiler filer = index.get(FilerIO.longBytes(fieldTermId));
        return new MiruOnDiskInvertedIndex<>(bitmaps, filer);
    }

    @Override
    public void bulkImport(MiruTenantId tenantId, BulkExport<Iterator<BulkEntry<Long, MiruInvertedIndex<BM>>>> importItems) throws Exception {
        Iterator<BulkEntry<Long, MiruInvertedIndex<BM>>> iter = importItems.bulkExport(tenantId);
        while (iter.hasNext()) {
            BulkEntry<Long, MiruInvertedIndex<BM>> entry = iter.next();
            SwappableFiler filer = index.get(FilerIO.longBytes(entry.key));

            final BM fieldTermIndex = entry.value.getIndex();
            MiruOnDiskInvertedIndex<BM> miruOnDiskInvertedIndex = new MiruOnDiskInvertedIndex<>(bitmaps, filer);
            miruOnDiskInvertedIndex.bulkImport(tenantId, new BulkExport<BM>() {
                @Override
                public BM bulkExport(MiruTenantId tenantId) throws Exception {
                    return fieldTermIndex;
                }
            });
        }
    }
}
