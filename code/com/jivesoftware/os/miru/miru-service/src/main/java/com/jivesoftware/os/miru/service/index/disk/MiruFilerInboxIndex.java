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
public class MiruFilerInboxIndex<BM> implements MiruInboxIndex<BM>,
    BulkExport<Void, KeyedInvertedIndexStream<BM>>,
    BulkImport<Void, KeyedInvertedIndexStream<BM>> {

    private final MiruBitmaps<BM> bitmaps;
    private final KeyedFilerStore store;
    private final StripingLocksProvider<MiruStreamId> stripingLocksProvider;

    public MiruFilerInboxIndex(MiruBitmaps<BM> bitmaps,
        KeyedFilerStore store,
        StripingLocksProvider<MiruStreamId> stripingLocksProvider)
        throws Exception {
        this.bitmaps = bitmaps;
        this.store = store;
        this.stripingLocksProvider = stripingLocksProvider;
    }

    @Override
    public void index(MiruStreamId streamId, int id) throws Exception {
        getAppender(streamId).append(id);
    }

    private MiruFilerInvertedIndex<BM> indexFor(MiruStreamId streamId) {
        return new MiruFilerInvertedIndex<>(bitmaps,
            store,
            streamId.getBytes(),
            -1,
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
    public void close() {
        store.close();
    }

    @Override
    public Void bulkExport(MiruTenantId tenantId, KeyedInvertedIndexStream<BM> callback) throws Exception {
        //TODO NEVER
        return null;
    }

    @Override
    public void bulkImport(final MiruTenantId tenantId, BulkExport<Void, KeyedInvertedIndexStream<BM>> export) throws Exception {
        export.bulkExport(tenantId, new KeyedInvertedIndexStream<BM>() {
            @Override
            public boolean stream(byte[] key, final MiruInvertedIndex<BM> importIndex) throws IOException {
                if (key == null) {
                    return true;
                }
                try {
                    Optional<BM> index = importIndex.getIndex();
                    if (index.isPresent()) {
                        MiruFilerInvertedIndex<BM> invertedIndex = new MiruFilerInvertedIndex<>(bitmaps,
                            store,
                            key,
                            -1,
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
