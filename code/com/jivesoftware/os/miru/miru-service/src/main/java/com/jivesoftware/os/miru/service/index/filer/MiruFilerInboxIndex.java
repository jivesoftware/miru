package com.jivesoftware.os.miru.service.index.filer;

import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.filer.map.store.api.KeyedFilerStore;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInboxIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndexAppender;

/** @author jonathan */
public class MiruFilerInboxIndex<BM> implements MiruInboxIndex<BM> {

    private final MiruBitmaps<BM> bitmaps;
    private final Cache<MiruFieldIndex.IndexKey, Optional<?>> fieldIndexCache;
    private final long indexId;
    private final KeyedFilerStore store;
    private final StripingLocksProvider<MiruStreamId> stripingLocksProvider;

    public MiruFilerInboxIndex(MiruBitmaps<BM> bitmaps,
        Cache<MiruFieldIndex.IndexKey, Optional<?>> fieldIndexCache,
        long indexId,
        KeyedFilerStore store,
        StripingLocksProvider<MiruStreamId> stripingLocksProvider)
        throws Exception {
        this.bitmaps = bitmaps;
        this.fieldIndexCache = fieldIndexCache;
        this.indexId = indexId;
        this.store = store;
        this.stripingLocksProvider = stripingLocksProvider;
    }

    @Override
    public void append(MiruStreamId streamId, int... ids) throws Exception {
        getAppender(streamId).append(ids);
    }

    public MiruFilerInvertedIndex<BM> getInbox(MiruStreamId streamId) {
        return new MiruFilerInvertedIndex<>(bitmaps,
            fieldIndexCache,
            new MiruFieldIndex.IndexKey(indexId, streamId.getBytes()),
            store,
            -1,
            stripingLocksProvider.lock(streamId));
    }

    @Override
    public MiruInvertedIndexAppender getAppender(MiruStreamId streamId) throws Exception {
        return getInbox(streamId);
    }

    @Override
    public int getLastActivityIndex(MiruStreamId streamId) throws Exception {
        return getInbox(streamId).lastId();
    }

    @Override
    public void close() {
        store.close();
    }
}
