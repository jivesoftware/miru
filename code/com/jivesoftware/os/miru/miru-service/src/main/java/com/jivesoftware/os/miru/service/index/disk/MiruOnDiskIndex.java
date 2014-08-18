package com.jivesoftware.os.miru.service.index.disk;

import com.google.common.base.Optional;
import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.jivesoftware.os.jive.utils.chunk.store.ChunkStore;
import com.jivesoftware.os.jive.utils.io.FilerIO;
import com.jivesoftware.os.jive.utils.keyed.store.FileBackedKeyedStore;
import com.jivesoftware.os.jive.utils.keyed.store.SwappableFiler;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.BulkImport;
import com.jivesoftware.os.miru.service.index.MiruIndex;
import com.jivesoftware.os.miru.service.index.MiruInvertedIndex;
import java.io.File;
import java.util.Map;

/** @author jonathan */
public class MiruOnDiskIndex implements MiruIndex, BulkImport<Map<Long, MiruInvertedIndex>> {

    private final FileBackedKeyedStore index;

    public MiruOnDiskIndex(File mapDirectory, File swapDirectory, ChunkStore chunkStore) throws Exception {
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
    public void allocate(int fieldId, int termId) throws Exception {
        getOrAllocate(fieldId, termId);
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
    public Optional<MiruInvertedIndex> get(int fieldId, int termId) throws Exception {
        long fieldTermId = FilerIO.bytesLong(FilerIO.intArrayToByteArray(new int[] { fieldId, termId }));
        SwappableFiler filer = index.get(FilerIO.longBytes(fieldTermId), false);
        if (filer == null) {
            return Optional.absent();
        }
        return Optional.<MiruInvertedIndex>of(new MiruOnDiskInvertedIndex(filer));
    }

    MiruInvertedIndex getOrAllocate(int fieldId, int termId) throws Exception {
        long fieldTermId = FilerIO.bytesLong(FilerIO.intArrayToByteArray(new int[] { fieldId, termId }));
        SwappableFiler filer = index.get(FilerIO.longBytes(fieldTermId));
        return new MiruOnDiskInvertedIndex(filer);
    }

    @Override
    public void bulkImport(BulkExport<Map<Long, MiruInvertedIndex>> importItems) throws Exception {
        Map<Long, MiruInvertedIndex> importMap = importItems.bulkExport();
        for (Map.Entry<Long, MiruInvertedIndex> entry : importMap.entrySet()) {
            SwappableFiler filer = index.get(FilerIO.longBytes(entry.getKey()));

            final EWAHCompressedBitmap fieldTermIndex = entry.getValue().getIndex();
            MiruOnDiskInvertedIndex miruOnDiskInvertedIndex = new MiruOnDiskInvertedIndex(filer);
            miruOnDiskInvertedIndex.bulkImport(new BulkExport<EWAHCompressedBitmap>() {
                @Override
                public EWAHCompressedBitmap bulkExport() throws Exception {
                    return fieldTermIndex;
                }
            });
        }
    }
}
