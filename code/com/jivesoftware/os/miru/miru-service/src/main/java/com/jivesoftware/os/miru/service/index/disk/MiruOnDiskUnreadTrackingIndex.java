package com.jivesoftware.os.miru.service.index.disk;

import com.google.common.base.Optional;
import com.jivesoftware.os.filer.chunk.store.MultiChunkStore;
import com.jivesoftware.os.filer.keyed.store.FileBackedKeyedStore;
import com.jivesoftware.os.filer.keyed.store.SwappableFiler;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndexAppender;
import com.jivesoftware.os.miru.plugin.index.MiruUnreadTrackingIndex;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.BulkImport;
import java.util.Collections;
import java.util.Map;

/** @author jonathan */
public class MiruOnDiskUnreadTrackingIndex<BM> implements MiruUnreadTrackingIndex<BM>, BulkImport<Map<MiruStreamId, MiruInvertedIndex<BM>>> {

    private final MiruBitmaps<BM> bitmaps;
    private final FileBackedKeyedStore index;
    private final long newFilerInitialCapacity = 512;

    public MiruOnDiskUnreadTrackingIndex(MiruBitmaps<BM> bitmaps, String[] mapDirectories, String[] swapDirectories, MultiChunkStore chunkStore)
        throws Exception {
        this.bitmaps = bitmaps;
        //TODO actual capacity? should this be shared with a key prefix?
        this.index = new FileBackedKeyedStore(mapDirectories, swapDirectories, 8, 100, chunkStore, 4);
    }

    @Override
    public void index(MiruStreamId streamId, int id) throws Exception {
        getAppender(streamId).append(id);
    }

    @Override
    public Optional<BM> getUnread(MiruStreamId streamId) throws Exception {
        SwappableFiler filer = index.get(streamId.getBytes(), -1);
        if (filer == null) {
            return Optional.absent();
        }
        return Optional.of(new MiruOnDiskInvertedIndex<>(bitmaps, filer).getIndex());
    }

    @Override
    public MiruInvertedIndexAppender getAppender(MiruStreamId streamId) throws Exception {
        return getOrCreateUnread(streamId);
    }

    private MiruInvertedIndex<BM> getOrCreateUnread(MiruStreamId streamId) throws Exception {
        SwappableFiler filer = index.get(streamId.getBytes(), newFilerInitialCapacity);
        return new MiruOnDiskInvertedIndex<>(bitmaps, filer);
    }

    @Override
    public void applyRead(MiruStreamId streamId, BM readMask) throws Exception {
        MiruInvertedIndex<BM> unread = getOrCreateUnread(streamId);
        unread.andNotToSourceSize(Collections.singletonList(readMask));
    }

    @Override
    public void applyUnread(MiruStreamId streamId, BM unreadMask) throws Exception {
        MiruInvertedIndex<BM> unread = getOrCreateUnread(streamId);
        unread.orToSourceSize(unreadMask);
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
    public void close() {
        index.close();
    }

    @Override
    public void bulkImport(MiruTenantId tenantId, BulkExport<Map<MiruStreamId, MiruInvertedIndex<BM>>> importItems) throws Exception {
        Map<MiruStreamId, MiruInvertedIndex<BM>> importIndex = importItems.bulkExport(tenantId);
        for (final Map.Entry<MiruStreamId, MiruInvertedIndex<BM>> entry : importIndex.entrySet()) {
            SwappableFiler filer = index.get(entry.getKey().getBytes(), newFilerInitialCapacity);

            MiruOnDiskInvertedIndex<BM> miruOnDiskInvertedIndex = new MiruOnDiskInvertedIndex<>(bitmaps, filer);
            miruOnDiskInvertedIndex.bulkImport(tenantId, new BulkExport<BM>() {
                @Override
                public BM bulkExport(MiruTenantId tenantId) throws Exception {
                    return entry.getValue().getIndex();
                }
            });
        }
    }
}
