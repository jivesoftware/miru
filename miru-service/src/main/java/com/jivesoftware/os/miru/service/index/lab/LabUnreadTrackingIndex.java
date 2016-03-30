package com.jivesoftware.os.miru.service.index.lab;

import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.lab.api.ValueIndex;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndexAppender;
import com.jivesoftware.os.miru.plugin.index.MiruUnreadTrackingIndex;
import com.jivesoftware.os.miru.plugin.partition.TrackError;
import java.util.Collections;

/** @author jonathan */
public class LabUnreadTrackingIndex<BM extends IBM, IBM> implements MiruUnreadTrackingIndex<BM, IBM> {

    private final OrderIdProvider idProvider;
    private final MiruBitmaps<BM, IBM> bitmaps;
    private final TrackError trackError;
    private final ValueIndex[] stores;
    private final StripingLocksProvider<MiruStreamId> stripingLocksProvider;

    public LabUnreadTrackingIndex(OrderIdProvider idProvider,
        MiruBitmaps<BM, IBM> bitmaps,
        TrackError trackError,
        ValueIndex[] stores,
        StripingLocksProvider<MiruStreamId> stripingLocksProvider)
        throws Exception {

        this.idProvider = idProvider;
        this.bitmaps = bitmaps;
        this.trackError = trackError;
        this.stores = stores;
        this.stripingLocksProvider = stripingLocksProvider;
    }

    private ValueIndex getStore(MiruStreamId streamId) {
        return stores[Math.abs(streamId.hashCode() % stores.length)];
    }

    @Override
    public void append(MiruStreamId streamId, StackBuffer stackBuffer, int... ids) throws Exception {
        getAppender(streamId).append(stackBuffer, ids);
    }

    @Override
    public MiruInvertedIndex<BM, IBM> getUnread(MiruStreamId streamId) throws Exception {
        return new LabInvertedIndex<>(idProvider,
            bitmaps,
            trackError,
            "unread",
            -1,
            streamId.getBytes(),
            getStore(streamId),
            stripingLocksProvider.lock(streamId, 0));
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
    public void close() throws Exception {
        for (ValueIndex store : stores) {
            store.close();
        }
    }
}
