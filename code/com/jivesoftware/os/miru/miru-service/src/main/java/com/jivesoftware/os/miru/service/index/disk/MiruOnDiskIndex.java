package com.jivesoftware.os.miru.service.index.disk;

import com.google.common.base.Optional;
import com.jivesoftware.os.jive.utils.chunk.store.ChunkStore;
import com.jivesoftware.os.jive.utils.io.FilerIO;
import com.jivesoftware.os.jive.utils.keyed.store.FileBackedKeyedStore;
import com.jivesoftware.os.jive.utils.keyed.store.SwappableFiler;
import com.jivesoftware.os.miru.service.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.BulkImport;
import com.jivesoftware.os.miru.service.index.MiruIndex;
import com.jivesoftware.os.miru.service.index.MiruInvertedIndex;
import java.io.File;
import java.util.Map;

/** @author jonathan */
public class MiruOnDiskIndex<BM> implements MiruIndex<BM>, BulkImport<Map<Long, MiruInvertedIndex<BM>>> {

    private final MiruBitmaps<BM> bitmaps;
    private final FileBackedKeyedStore index;

    public MiruOnDiskIndex(MiruBitmaps<BM> bitmaps, File mapDirectory, File swapDirectory, ChunkStore chunkStore) throws Exception {
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
    public MiruInvertedIndex allocate(int fieldId, int termId) throws Exception {
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

    MiruInvertedIndex getOrAllocate(int fieldId, int termId) throws Exception {
        long fieldTermId = FilerIO.bytesLong(FilerIO.intArrayToByteArray(new int[] { fieldId, termId }));
        SwappableFiler filer = index.get(FilerIO.longBytes(fieldTermId));
        return new MiruOnDiskInvertedIndex(bitmaps, filer);
    }

    @Override
    public void bulkImport(BulkExport<Map<Long, MiruInvertedIndex<BM>>> importItems) throws Exception {
        Map<Long, MiruInvertedIndex<BM>> importMap = importItems.bulkExport();
        for (Map.Entry<Long, MiruInvertedIndex<BM>> entry : importMap.entrySet()) {
            SwappableFiler filer = index.get(FilerIO.longBytes(entry.getKey()));

            final BM fieldTermIndex = entry.getValue().getIndex();
            MiruOnDiskInvertedIndex miruOnDiskInvertedIndex = new MiruOnDiskInvertedIndex(bitmaps, filer);
            miruOnDiskInvertedIndex.bulkImport(new BulkExport<BM>() {
                @Override
                public BM bulkExport() throws Exception {
                    return fieldTermIndex;
                }
            });
        }
    }
}
