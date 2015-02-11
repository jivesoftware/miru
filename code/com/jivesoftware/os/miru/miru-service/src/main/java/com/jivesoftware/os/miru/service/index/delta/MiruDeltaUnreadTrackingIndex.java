package com.jivesoftware.os.miru.service.index.delta;

import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndexAppender;
import com.jivesoftware.os.miru.plugin.index.MiruUnreadTrackingIndex;
import java.util.Collections;
import java.util.concurrent.ConcurrentMap;

/** @author jonathan */
public class MiruDeltaUnreadTrackingIndex<BM> implements MiruUnreadTrackingIndex<BM> {

    private final MiruBitmaps<BM> bitmaps;
    private final MiruUnreadTrackingIndex<BM> backingIndex;
    private final ConcurrentMap<MiruStreamId, MiruDeltaInvertedIndex<BM>> unreadDeltas = Maps.newConcurrentMap();

    public MiruDeltaUnreadTrackingIndex(MiruBitmaps<BM> bitmaps, MiruUnreadTrackingIndex<BM> backingIndex) {
        this.bitmaps = bitmaps;
        this.backingIndex = backingIndex;
    }

    @Override
    public void append(MiruStreamId streamId, int... ids) throws Exception {
        getAppender(streamId).append(ids);
    }

    @Override
    public MiruInvertedIndex<BM> getUnread(MiruStreamId streamId) throws Exception {
        MiruDeltaInvertedIndex<BM> delta = unreadDeltas.get(streamId);
        if (delta == null) {
            delta = new MiruDeltaInvertedIndex<>(bitmaps, backingIndex.getUnread(streamId), new MiruDeltaInvertedIndex.Delta<BM>());
            MiruDeltaInvertedIndex<BM> existing = unreadDeltas.putIfAbsent(streamId, delta);
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
        backingIndex.close();
    }

    public void merge() throws Exception {
        for (MiruDeltaInvertedIndex<BM> delta : unreadDeltas.values()) {
            delta.merge();
        }
        unreadDeltas.clear();
    }
}
