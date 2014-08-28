package com.jivesoftware.os.miru.service.index.disk;

import com.google.common.base.Optional;
import com.jivesoftware.os.jive.utils.chunk.store.MultiChunkStore;
import com.jivesoftware.os.jive.utils.io.Filer;
import com.jivesoftware.os.jive.utils.io.FilerIO;
import com.jivesoftware.os.jive.utils.keyed.store.FileBackedKeyedStore;
import com.jivesoftware.os.jive.utils.keyed.store.SwappableFiler;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.query.MiruBitmaps;
import com.jivesoftware.os.miru.query.MiruInboxIndex;
import com.jivesoftware.os.miru.query.MiruInvertedIndex;
import com.jivesoftware.os.miru.query.MiruInvertedIndexAppender;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.BulkImport;
import com.jivesoftware.os.miru.service.index.memory.MiruInMemoryInboxIndex.InboxAndLastActivityIndex;
import java.io.File;
import java.util.Map;

/** @author jonathan */
public class MiruOnDiskInboxIndex<BM> implements MiruInboxIndex<BM>, BulkImport<InboxAndLastActivityIndex<BM>> {

    private final MiruBitmaps<BM> bitmaps;
    private final FileBackedKeyedStore index;

    public MiruOnDiskInboxIndex(MiruBitmaps<BM> bitmaps, File mapDirectory, File swapDirectory, MultiChunkStore chunkStore) throws Exception {
        this.bitmaps = bitmaps;
        //TODO actual capacity? should this be shared with a key prefix?
        this.index = new FileBackedKeyedStore(mapDirectory.getAbsolutePath(), swapDirectory.getAbsolutePath(), 8, 100, chunkStore, 512);
    }

    @Override
    public void index(MiruStreamId streamId, int id) throws Exception {
        getAppender(streamId).append(id);
    }

    @Override
    public Optional<BM> getInbox(MiruStreamId streamId) throws Exception {
        SwappableFiler filer = index.get(streamId.getBytes(), false);
        if (filer == null) {
            return Optional.absent();
        }
        return Optional.<BM>of(new MiruOnDiskInvertedIndex<>(bitmaps, filer, 4).getIndex());
    }

    @Override
    public MiruInvertedIndexAppender getAppender(MiruStreamId streamId) throws Exception {
        SwappableFiler filer = index.get(streamId.getBytes(), false);
        if (filer == null) {
            filer = index.get(streamId.getBytes(), true);
            setLastActivityIndex(streamId, -1); // Initialize lastActivityIndex to -1 when we create the on-disk index
        }
        return new MiruOnDiskInvertedIndex<>(bitmaps, filer, 4);
    }

    @Override
    public int getLastActivityIndex(MiruStreamId streamId) throws Exception {
        Filer filer = index.get(streamId.getBytes(), false);
        if (filer == null) {
            return -1;
        }

        synchronized (filer.lock()) {
            filer.seek(0);
            return FilerIO.readInt(filer, "lastActivityIndex");
        }
    }

    @Override
    public void setLastActivityIndex(MiruStreamId streamId, int activityIndex) throws Exception {
        Filer filer = index.get(streamId.getBytes(), true);

        synchronized (filer.lock()) {
            filer.seek(0);
            FilerIO.writeInt(filer, activityIndex, "lastActivityIndex");
        }
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
    public void bulkImport(MiruTenantId tenantId, BulkExport<InboxAndLastActivityIndex<BM>> importItems) throws Exception {
        InboxAndLastActivityIndex<BM> bulkImport = importItems.bulkExport(tenantId);

        for (final Map.Entry<MiruStreamId, MiruInvertedIndex<BM>> entry : bulkImport.index.entrySet()) {
            SwappableFiler filer = index.get(entry.getKey().getBytes(), true);

            synchronized (filer.lock()) {
                filer.sync();
                filer.seek(0);
                if (bulkImport.lastActivityIndex.containsKey(entry.getKey())) {
                    FilerIO.writeInt(filer, bulkImport.lastActivityIndex.get(entry.getKey()), "lastActivityIndex");
                } else {
                    FilerIO.writeInt(filer, -1, "lastActivityIndex"); // Initialize lastActivityIndex to -1 if no value exists
                }
            }

            MiruOnDiskInvertedIndex<BM> miruOnDiskInvertedIndex = new MiruOnDiskInvertedIndex<>(bitmaps, filer, 4);
            miruOnDiskInvertedIndex.bulkImport(tenantId, new BulkExport<BM>() {
                @Override
                public BM bulkExport(MiruTenantId tenantId) throws Exception {
                    return entry.getValue().getIndex();
                }
            });
        }

        // If for some reason we didn't have an inverted index for a given streamId, handle it here
        for (Map.Entry<MiruStreamId, Integer> entry : bulkImport.lastActivityIndex.entrySet()) {
            if (bulkImport.index.containsKey(entry.getKey())) {
                continue; // Already handled above
            }
            Filer filer = index.get(entry.getKey().getBytes(), true);

            synchronized (filer.lock()) {
                filer.seek(0);
                FilerIO.writeInt(filer, entry.getValue(), "lastActivityIndex");
            }
        }
    }
}
