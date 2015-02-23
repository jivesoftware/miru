package com.jivesoftware.os.miru.service.index.filer;

import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.filer.io.api.KeyedFilerStore;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndexAppender;
import com.jivesoftware.os.miru.plugin.index.MiruUnreadTrackingIndex;
import java.util.Collections;

/** @author jonathan */
public class MiruFilerUnreadTrackingIndex<BM> implements MiruUnreadTrackingIndex<BM> {

    private final MiruBitmaps<BM> bitmaps;
    private final long indexId;
    private final KeyedFilerStore store;
    private final StripingLocksProvider<MiruStreamId> stripingLocksProvider;

    public MiruFilerUnreadTrackingIndex(MiruBitmaps<BM> bitmaps,
        long indexId,
        KeyedFilerStore store,
        StripingLocksProvider<MiruStreamId> stripingLocksProvider)
        throws Exception {
        this.bitmaps = bitmaps;
        this.indexId = indexId;
        this.store = store;
        this.stripingLocksProvider = stripingLocksProvider;
    }

    @Override
    public void append(MiruStreamId streamId, int... ids) throws Exception {
        getAppender(streamId).append(ids);
    }

    @Override
    public MiruInvertedIndex<BM> getUnread(MiruStreamId streamId) throws Exception {
        return new MiruFilerInvertedIndex<>(bitmaps, new MiruFieldIndex.IndexKey(indexId, streamId.getBytes()), store, -1,
            stripingLocksProvider.lock(streamId));
    }

    @Override
    public MiruInvertedIndexAppender getAppender(MiruStreamId streamId) throws Exception {
        return getUnread(streamId);
    }

    @Override
    public void applyRead(MiruStreamId streamId, BM readMask) throws Exception {
        MiruInvertedIndex<BM> unread = getUnread(streamId);
        unread.andNotToSourceSize(Collections.singletonList(readMask));
    }

    @Override
    public void applyUnread(MiruStreamId streamId, BM unreadMask) throws Exception {
        MiruInvertedIndex<BM> unread = getUnread(streamId);
        unread.orToSourceSize(unreadMask);
    }

    @Override
    public void close() {
        store.close();
    }
}
