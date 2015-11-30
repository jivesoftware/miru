package com.jivesoftware.os.miru.service.index.delta;

import com.google.common.collect.Maps;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndexAppender;
import com.jivesoftware.os.miru.plugin.index.MiruUnreadTrackingIndex;
import com.jivesoftware.os.miru.service.index.Mergeable;
import java.util.Collections;
import java.util.concurrent.ConcurrentMap;

/** @author jonathan */
public class MiruDeltaUnreadTrackingIndex<BM extends IBM, IBM> implements MiruUnreadTrackingIndex<IBM>, Mergeable {

    private final MiruBitmaps<BM, IBM> bitmaps;
    private final MiruUnreadTrackingIndex<IBM> backingIndex;
    private final ConcurrentMap<MiruStreamId, MiruDeltaInvertedIndex<BM, IBM>> unreadDeltas = Maps.newConcurrentMap();

    public MiruDeltaUnreadTrackingIndex(MiruBitmaps<BM, IBM> bitmaps,
        MiruUnreadTrackingIndex<IBM> backingIndex) {
        this.bitmaps = bitmaps;
        this.backingIndex = backingIndex;
    }

    @Override
    public void append(MiruStreamId streamId, StackBuffer stackBuffer, int... ids) throws Exception {
        getAppender(streamId).append(stackBuffer, ids);
    }

    @Override
    public MiruInvertedIndex<IBM> getUnread(MiruStreamId streamId) throws Exception {
        MiruDeltaInvertedIndex<BM, IBM> delta = unreadDeltas.get(streamId);
        if (delta == null) {
            delta = new MiruDeltaInvertedIndex<>(bitmaps, backingIndex.getUnread(streamId), new MiruDeltaInvertedIndex.Delta<>());
            MiruDeltaInvertedIndex<BM, IBM> existing = unreadDeltas.putIfAbsent(streamId, delta);
            if (existing != null) {
                delta = existing;
            }
        }
        return delta;
    }

    @Override
    public MiruInvertedIndexAppender getAppender(MiruStreamId streamId) throws Exception {
        return getUnread(streamId);
    }

    @Override
    public void applyRead(MiruStreamId streamId, IBM readMask, StackBuffer stackBuffer) throws Exception {
        MiruInvertedIndex<IBM> unread = getUnread(streamId);
        unread.andNotToSourceSize(Collections.singletonList(readMask), stackBuffer);
    }

    @Override
    public void applyUnread(MiruStreamId streamId, IBM unreadMask, StackBuffer stackBuffer) throws Exception {
        MiruInvertedIndex<IBM> unread = getUnread(streamId);
        unread.orToSourceSize(unreadMask, stackBuffer);
    }

    @Override
    public void close() {
        backingIndex.close();
    }

    @Override
    public void merge(StackBuffer stackBuffer) throws Exception {
        for (MiruDeltaInvertedIndex<BM, IBM> delta : unreadDeltas.values()) {
            delta.merge(stackBuffer);
        }
        unreadDeltas.clear();
    }
}
