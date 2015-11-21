package com.jivesoftware.os.miru.service.index.filer;

import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.filer.io.api.KeyedFilerStore;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInboxIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndexAppender;

/** @author jonathan */
public class MiruFilerInboxIndex<BM extends IBM, IBM> implements MiruInboxIndex<IBM> {

    private final MiruBitmaps<BM, IBM> bitmaps;
    private final long indexId;
    private final KeyedFilerStore<Long, Void> store;
    private final StripingLocksProvider<MiruStreamId> stripingLocksProvider;

    public MiruFilerInboxIndex(MiruBitmaps<BM, IBM> bitmaps,
        long indexId,
        KeyedFilerStore<Long, Void> store,
        StripingLocksProvider<MiruStreamId> stripingLocksProvider)
        throws Exception {
        this.bitmaps = bitmaps;
        this.indexId = indexId;
        this.store = store;
        this.stripingLocksProvider = stripingLocksProvider;
    }

    @Override
    public void append(MiruStreamId streamId, byte[] primitiveBuffer, int... ids) throws Exception {
        getAppender(streamId).append(primitiveBuffer, ids);
    }

    @Override
    public MiruInvertedIndex<IBM> getInbox(MiruStreamId streamId) {
        return new MiruFilerInvertedIndex<>(bitmaps,
            new MiruFieldIndex.IndexKey(indexId, streamId.getBytes()),
            store,
            -1,
            stripingLocksProvider.lock(streamId, 0));
    }

    @Override
    public MiruInvertedIndexAppender getAppender(MiruStreamId streamId) throws Exception {
        return getInbox(streamId);
    }

    @Override
    public int getLastActivityIndex(MiruStreamId streamId, byte[] primitiveBuffer) throws Exception {
        return getInbox(streamId).lastId(primitiveBuffer);
    }

    @Override
    public void close() {
        store.close();
    }
}
