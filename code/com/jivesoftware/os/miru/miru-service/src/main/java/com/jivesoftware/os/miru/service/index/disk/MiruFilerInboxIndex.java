package com.jivesoftware.os.miru.service.index.disk;

import com.google.common.base.Optional;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.filer.keyed.store.KeyedFilerStore;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruInboxIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndexAppender;

/** @author jonathan */
public class MiruFilerInboxIndex<BM> implements MiruInboxIndex<BM> {

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
}
