package com.jivesoftware.os.miru.service.index.delta;

import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruInboxIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndexAppender;
import java.util.concurrent.ConcurrentMap;

/**
 * DELTA FORCE
 */
public class MiruDeltaInboxIndex<BM> implements MiruInboxIndex<BM> {

    private final MiruBitmaps<BM> bitmaps;
    private final MiruInboxIndex<BM> backingIndex;
    private final ConcurrentMap<MiruStreamId, MiruDeltaInvertedIndex<BM>> inboxDeltas = Maps.newConcurrentMap();

    public MiruDeltaInboxIndex(MiruBitmaps<BM> bitmaps, MiruInboxIndex<BM> backingIndex) {
        this.bitmaps = bitmaps;
        this.backingIndex = backingIndex;
    }

    @Override
    public void append(MiruStreamId streamId, int... ids) throws Exception {
        getAppender(streamId).append(ids);
    }

    @Override
    public MiruInvertedIndex<BM> getInbox(MiruStreamId streamId) throws Exception {
        MiruDeltaInvertedIndex<BM> delta = inboxDeltas.get(streamId);
        if (delta == null) {
            delta = new MiruDeltaInvertedIndex<>(bitmaps, backingIndex.getInbox(streamId), new MiruDeltaInvertedIndex.Delta<BM>());
            MiruDeltaInvertedIndex<BM> existing = inboxDeltas.putIfAbsent(streamId, delta);
            if (existing != null) {
                delta = existing;
            }
        }
        return delta;
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
        backingIndex.close();
    }

    public void merge() throws Exception {
        for (MiruDeltaInvertedIndex<BM> delta : inboxDeltas.values()) {
            delta.merge();
        }
        inboxDeltas.clear();
    }
}
