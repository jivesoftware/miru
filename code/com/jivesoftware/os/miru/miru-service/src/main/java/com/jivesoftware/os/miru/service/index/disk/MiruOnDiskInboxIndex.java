package com.jivesoftware.os.miru.service.index.disk;

import com.google.common.base.Optional;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.filer.keyed.store.KeyedFilerStore;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruInboxIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndexAppender;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.BulkImport;
import com.jivesoftware.os.miru.service.index.SimpleBulkExport;
import com.jivesoftware.os.miru.service.index.memory.KeyedInvertedIndexStream;
import java.io.IOException;

/** @author jonathan */
public class MiruOnDiskInboxIndex<BM> implements MiruInboxIndex<BM>, BulkImport<Void, KeyedInvertedIndexStream<BM>> {

    private final MiruBitmaps<BM> bitmaps;
    private final KeyedFilerStore store;
    private final StripingLocksProvider<MiruStreamId> stripingLocksProvider = new StripingLocksProvider<>(64);
    private final long newFilerInitialCapacity = 512; //TODO expose to config

    public MiruOnDiskInboxIndex(MiruBitmaps<BM> bitmaps,
        KeyedFilerStore store)
        throws Exception {
        this.bitmaps = bitmaps;
        this.store = store;
        /*
        String[] mapDirectories,
        String[] swapDirectories,
        MultiChunkStore chunkStore,
        StripingLocksProvider<String> keyedStoreStripingLocksProvider
        //TODO actual capacity? should this be shared with a key prefix?
        this.index = new PartitionedMapChunkBackedKeyedStore(
            new FileBackedMapChunkFactory(8, false, 8, false, 100, mapDirectories),
            new FileBackedMapChunkFactory(8, false, 8, false, 100, swapDirectories),
            chunkStore,
            keyedStoreStripingLocksProvider,
            4); //TODO expose number of partitions
        */
    }

    @Override
    public void index(MiruStreamId streamId, int id) throws Exception {
        getAppender(streamId).append(id);
    }

    private MiruOnDiskInvertedIndex<BM> indexFor(MiruStreamId streamId) {
        return new MiruOnDiskInvertedIndex<>(bitmaps,
            store,
            streamId.getBytes(),
            -1,
            newFilerInitialCapacity,
            stripingLocksProvider.lock(streamId));
    }

    @Override
    public Optional<BM> getInbox(MiruStreamId streamId) throws Exception {
        return indexFor(streamId).getIndex();
    }

    @Override
    public MiruInvertedIndexAppender getAppender(MiruStreamId streamId) throws Exception {
        return indexFor(streamId);
    }

    @Override
    public int getLastActivityIndex(MiruStreamId streamId) throws Exception {
        return indexFor(streamId).lastId();
    }

    @Override
    public long sizeInMemory() throws Exception {
        return 0;
    }

    @Override
    public long sizeOnDisk() throws Exception {
        return 0;
    }

    @Override
    public void close() {
        store.close();
    }

    @Override
    public void bulkImport(final MiruTenantId tenantId, BulkExport<Void, KeyedInvertedIndexStream<BM>> export) throws Exception {
        export.bulkExport(tenantId, new KeyedInvertedIndexStream<BM>() {
            @Override
            public boolean stream(byte[] key, final MiruInvertedIndex<BM> importIndex) throws IOException {
                try {
                    Optional<BM> index = importIndex.getIndex();
                    if (index.isPresent()) {
                        long importFilerCapacity = MiruOnDiskInvertedIndex.serializedSizeInBytes(bitmaps, index.get());
                        MiruOnDiskInvertedIndex<BM> invertedIndex = new MiruOnDiskInvertedIndex<>(bitmaps,
                            store,
                            key,
                            -1,
                            importFilerCapacity,
                            new Object());
                        invertedIndex.bulkImport(tenantId, new SimpleBulkExport<>(importIndex));
                    }
                    return true;
                } catch (Exception e) {
                    throw new IOException("Failed to stream import", e);
                }
            }
        });
    }
}
