package com.jivesoftware.os.miru.service.index.delta;

import com.google.common.collect.Maps;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruInboxIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndexAppender;
import com.jivesoftware.os.miru.plugin.partition.TrackError;
import com.jivesoftware.os.miru.service.index.Mergeable;
import java.util.concurrent.ConcurrentMap;

/**
 * DELTA FORCE
 */
public class MiruDeltaInboxIndex<BM extends IBM, IBM> implements MiruInboxIndex<BM, IBM>, Mergeable {

    private final MiruBitmaps<BM, IBM> bitmaps;
    private final TrackError trackError;
    private final MiruInboxIndex<BM, IBM> backingIndex;
    private final ConcurrentMap<MiruStreamId, MiruDeltaInvertedIndex<BM, IBM>> inboxDeltas = Maps.newConcurrentMap();

    public MiruDeltaInboxIndex(MiruBitmaps<BM, IBM> bitmaps,
        TrackError trackError,
        MiruInboxIndex<BM, IBM> backingIndex) {
        this.bitmaps = bitmaps;
        this.trackError = trackError;
        this.backingIndex = backingIndex;
    }

    @Override
    public void append(MiruStreamId streamId, StackBuffer stackBuffer, int... ids) throws Exception {
        getAppender(streamId).append(stackBuffer, ids);
    }

    @Override
    public MiruInvertedIndex<BM, IBM> getInbox(MiruStreamId streamId) throws Exception {
        MiruDeltaInvertedIndex<BM, IBM> delta = inboxDeltas.get(streamId);
        if (delta == null) {
            delta = new MiruDeltaInvertedIndex<>(bitmaps, trackError, backingIndex.getInbox(streamId), new MiruDeltaInvertedIndex.Delta<>());
            MiruDeltaInvertedIndex<BM, IBM> existing = inboxDeltas.putIfAbsent(streamId, delta);
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
    public int getLastActivityIndex(MiruStreamId streamId, StackBuffer stackBuffer) throws Exception {
        return getInbox(streamId).lastId(stackBuffer);
    }

    @Override
    public void close() {
        backingIndex.close();
    }

    @Override
    public void merge(StackBuffer stackBuffer) throws Exception {
        for (MiruDeltaInvertedIndex<BM, IBM> delta : inboxDeltas.values()) {
            delta.merge(stackBuffer);
        }
        inboxDeltas.clear();
    }
}
