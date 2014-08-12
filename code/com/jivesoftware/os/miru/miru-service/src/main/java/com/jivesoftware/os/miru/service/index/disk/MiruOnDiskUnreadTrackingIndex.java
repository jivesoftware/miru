package com.jivesoftware.os.miru.service.index.disk;

import com.google.common.base.Optional;
import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.BulkImport;
import com.jivesoftware.os.miru.service.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.service.index.MiruInvertedIndexAppender;
import com.jivesoftware.os.miru.service.index.MiruUnreadTrackingIndex;
import com.jivesoftware.os.jive.utils.chunk.store.ChunkStore;
import com.jivesoftware.os.jive.utils.keyed.store.FileBackedKeyedStore;
import com.jivesoftware.os.jive.utils.keyed.store.SwappableFiler;
import java.io.File;
import java.util.Map;

/** @author jonathan */
public class MiruOnDiskUnreadTrackingIndex implements MiruUnreadTrackingIndex, BulkImport<Map<MiruStreamId, MiruInvertedIndex>> {

    private final FileBackedKeyedStore index;

    public MiruOnDiskUnreadTrackingIndex(File mapDirectory, File swapDirectory, ChunkStore chunkStore) throws Exception {
        //TODO actual capacity? should this be shared with a key prefix?
        this.index = new FileBackedKeyedStore(mapDirectory.getAbsolutePath(), swapDirectory.getAbsolutePath(), 8, 100, chunkStore, 512);
    }

    @Override
    public void index(MiruStreamId streamId, int id) throws Exception {
        getAppender(streamId).append(id);
    }

    @Override
    public Optional<EWAHCompressedBitmap> getUnread(MiruStreamId streamId) throws Exception {
        SwappableFiler filer = index.get(streamId.getBytes(), false);
        if (filer == null) {
            return Optional.absent();
        }
        return Optional.<EWAHCompressedBitmap>of(new MiruOnDiskInvertedIndex(filer).getIndex());
    }

    @Override
    public MiruInvertedIndexAppender getAppender(MiruStreamId streamId) throws Exception {
        return getOrCreateUnread(streamId);
    }

    private MiruInvertedIndex getOrCreateUnread(MiruStreamId streamId) throws Exception {
        SwappableFiler filer = index.get(streamId.getBytes(), true);
        return new MiruOnDiskInvertedIndex(filer);
    }

    @Override
    public void applyRead(MiruStreamId streamId, EWAHCompressedBitmap readMask) throws Exception {
        MiruInvertedIndex unread = getOrCreateUnread(streamId);
        unread.andNot(readMask);
    }

    @Override
    public void applyUnread(MiruStreamId streamId, EWAHCompressedBitmap unreadMask) throws Exception {
        MiruInvertedIndex unread = getOrCreateUnread(streamId);
        unread.or(unreadMask);
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
    public void bulkImport(BulkExport<Map<MiruStreamId, MiruInvertedIndex>> importItems) throws Exception {
        Map<MiruStreamId, MiruInvertedIndex> importIndex = importItems.bulkExport();
        for (final Map.Entry<MiruStreamId, MiruInvertedIndex> entry : importIndex.entrySet()) {
            SwappableFiler filer = index.get(entry.getKey().getBytes(), true);

            MiruOnDiskInvertedIndex miruOnDiskInvertedIndex = new MiruOnDiskInvertedIndex(filer);
            miruOnDiskInvertedIndex.bulkImport(new BulkExport<EWAHCompressedBitmap>() {
                @Override
                public EWAHCompressedBitmap bulkExport() throws Exception {
                    return entry.getValue().getIndex();
                }
            });
        }
    }
}
