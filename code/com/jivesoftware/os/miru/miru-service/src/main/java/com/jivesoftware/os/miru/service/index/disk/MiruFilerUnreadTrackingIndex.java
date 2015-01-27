package com.jivesoftware.os.miru.service.index.disk;

import com.google.common.base.Optional;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.filer.map.store.api.KeyedFilerStore;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndexAppender;
import com.jivesoftware.os.miru.plugin.index.MiruUnreadTrackingIndex;
import java.util.Collections;

/** @author jonathan */
public class MiruFilerUnreadTrackingIndex<BM> implements MiruUnreadTrackingIndex<BM> {

    private final MiruBitmaps<BM> bitmaps;
    private final KeyedFilerStore store;
    private final StripingLocksProvider<MiruStreamId> stripingLocksProvider;

    public MiruFilerUnreadTrackingIndex(MiruBitmaps<BM> bitmaps,
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

    @Override
    public Optional<BM> getUnread(MiruStreamId streamId) throws Exception {
        return new MiruFilerInvertedIndex<>(bitmaps, store, streamId.getBytes(), -1, stripingLocksProvider.lock(streamId)).getIndex();
    }

    @Override
    public MiruInvertedIndexAppender getAppender(MiruStreamId streamId) throws Exception {
        return getOrCreateUnread(streamId);
    }

    private MiruInvertedIndex<BM> getOrCreateUnread(MiruStreamId streamId) throws Exception {
        return new MiruFilerInvertedIndex<>(bitmaps, store, streamId.getBytes(), -1, stripingLocksProvider.lock(streamId));
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
    public void close() {
        store.close();
    }
}
