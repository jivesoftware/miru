package com.jivesoftware.os.miru.service.index.filer;

import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.filer.io.api.KeyedFilerStore;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndexAppender;
import com.jivesoftware.os.miru.plugin.index.MiruUnreadTrackingIndex;
import com.jivesoftware.os.miru.plugin.partition.TrackError;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Collections;

/** @author jonathan */
public class MiruFilerUnreadTrackingIndex<BM extends IBM, IBM> implements MiruUnreadTrackingIndex<BM, IBM> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruBitmaps<BM, IBM> bitmaps;
    private final TrackError trackError;
    private final KeyedFilerStore<Long, Void> store;
    private final StripingLocksProvider<MiruStreamId> stripingLocksProvider;

    public MiruFilerUnreadTrackingIndex(MiruBitmaps<BM, IBM> bitmaps,
        TrackError trackError,
        KeyedFilerStore<Long, Void> store,
        StripingLocksProvider<MiruStreamId> stripingLocksProvider)
        throws Exception {
        this.bitmaps = bitmaps;
        this.trackError = trackError;
        this.store = store;
        this.stripingLocksProvider = stripingLocksProvider;
    }

    @Override
    public void append(MiruStreamId streamId, StackBuffer stackBuffer, int... ids) throws Exception {
        getAppender(streamId).set(stackBuffer, ids);
    }

    @Override
    public MiruInvertedIndex<BM, IBM> getUnread(MiruStreamId streamId) throws Exception {
        return new MiruFilerInvertedIndex<>(bitmaps, trackError, "unread", -1, streamId.getBytes(), store, stripingLocksProvider.lock(streamId, 0));
    }

    @Override
    public MiruInvertedIndexAppender getAppender(MiruStreamId streamId) throws Exception {
        return getUnread(streamId);
    }

    @Override
    public void applyRead(MiruStreamId streamId, IBM readMask, StackBuffer stackBuffer) throws Exception {
        MiruInvertedIndex<BM, IBM> unread = getUnread(streamId);
        unread.andNotToSourceSize(Collections.singletonList(readMask), stackBuffer);
    }

    @Override
    public void applyUnread(MiruStreamId streamId, IBM unreadMask, StackBuffer stackBuffer) throws Exception {
        MiruInvertedIndex<BM, IBM> unread = getUnread(streamId);
        unread.orToSourceSize(unreadMask, stackBuffer);
    }

    @Override
    public void close() {
        store.close();
    }

    @Override
    public int getLastActivityIndex(MiruStreamId streamId, StackBuffer stackBuffer) throws Exception {
        LOG.error("Deprecated usage of filer getLastActivityIndex");
        return -1;
    }

    @Override
    public void setLastActivityIndex(MiruStreamId streamId, int lastActivityIndex, StackBuffer stackBuffer) throws Exception {
        LOG.error("Deprecated usage of filer setLastActivityIndex");
    }
}
