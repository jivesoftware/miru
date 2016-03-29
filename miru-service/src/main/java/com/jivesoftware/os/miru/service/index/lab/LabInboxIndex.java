package com.jivesoftware.os.miru.service.index.lab;

import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.lab.api.ValueIndex;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruInboxIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndexAppender;
import com.jivesoftware.os.miru.plugin.partition.TrackError;

/** @author jonathan */
public class LabInboxIndex<BM extends IBM, IBM> implements MiruInboxIndex<BM, IBM> {

    private final OrderIdProvider idProvider;
    private final MiruBitmaps<BM, IBM> bitmaps;
    private final TrackError trackError;
    private final ValueIndex store;
    private final StripingLocksProvider<MiruStreamId> stripingLocksProvider;

    public LabInboxIndex(OrderIdProvider idProvider,
        MiruBitmaps<BM, IBM> bitmaps,
        TrackError trackError,
        ValueIndex store,
        StripingLocksProvider<MiruStreamId> stripingLocksProvider)
        throws Exception {

        this.idProvider = idProvider;
        this.bitmaps = bitmaps;
        this.trackError = trackError;
        this.store = store;
        this.stripingLocksProvider = stripingLocksProvider;
    }

    @Override
    public void append(MiruStreamId streamId, StackBuffer stackBuffer, int... ids) throws Exception {
        getAppender(streamId).append(stackBuffer, ids);
    }

    @Override
    public MiruInvertedIndex<BM, IBM> getInbox(MiruStreamId streamId) {
        return new LabInvertedIndex<>(idProvider,
            bitmaps,
            trackError,
            "inbox",
            -2,
            streamId.getBytes(),
            store,
            stripingLocksProvider.lock(streamId, 0));
    }

    @Override
    public MiruInvertedIndexAppender getAppender(MiruStreamId streamId) throws Exception {
        return getInbox(streamId);
    }

    @Override
    public int getLastActivityIndex(MiruStreamId streamId, StackBuffer stackBuffer) throws Exception {
        return getInbox(streamId).lastId(stackBuffer);
    }

    @Override
    public void close() throws Exception {
        store.close();
    }
}
