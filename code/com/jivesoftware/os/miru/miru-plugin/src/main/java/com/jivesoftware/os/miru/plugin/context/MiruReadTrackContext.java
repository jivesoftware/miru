package com.jivesoftware.os.miru.plugin.context;

import com.google.common.base.Optional;
import com.jivesoftware.os.jive.utils.base.util.locks.StripingLocksProvider;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruFields;
import com.jivesoftware.os.miru.plugin.index.MiruTimeIndex;
import com.jivesoftware.os.miru.plugin.index.MiruUnreadTrackingIndex;
import com.jivesoftware.os.miru.plugin.solution.MiruAggregateUtil;
import java.util.Arrays;

/** @author jonathan */
public class MiruReadTrackContext<BM> {

    private final MiruBitmaps<BM> bitmaps;
    private final MiruSchema schema;
    private final MiruFields<BM> fieldIndex;
    private final MiruTimeIndex timeIndex;
    private final MiruUnreadTrackingIndex<BM> unreadTrackingIndex;
    private final StripingLocksProvider<MiruStreamId> streamLocks;
    private final MiruAggregateUtil aggregateUtil = new MiruAggregateUtil();

    public MiruReadTrackContext(MiruBitmaps<BM> bitmaps,
            MiruSchema schema,
            MiruFields<BM> fieldIndex,
            MiruTimeIndex timeIndex,
            MiruUnreadTrackingIndex<BM> unreadTrackingIndex,
            StripingLocksProvider<MiruStreamId> streamLocks) {

        this.bitmaps = bitmaps;
        this.schema = schema;
        this.fieldIndex = fieldIndex;
        this.timeIndex = timeIndex;
        this.unreadTrackingIndex = unreadTrackingIndex;
        this.streamLocks = streamLocks;
    }

    public void read(MiruStreamId streamId, MiruFilter filter, int lastActivityIndex, long lastActivityTimestamp) throws Exception {
        BM indexMask = bitmaps.buildIndexMask(lastActivityIndex, Optional.<BM>absent());

        synchronized (streamLocks.lock(streamId)) {
            BM timeMask = bitmaps.buildTimeRangeMask(timeIndex, 0L, lastActivityTimestamp);
            BM filtered = bitmaps.create();
            aggregateUtil.filter(bitmaps, schema, fieldIndex, filter, filtered, -1);

            BM result = bitmaps.create();
            bitmaps.and(result, Arrays.asList(filtered, indexMask, timeMask));
            unreadTrackingIndex.applyRead(streamId, result);
        }
    }

    public void unread(MiruStreamId streamId, MiruFilter filter, int lastActivityIndex, long lastActivityTimestamp) throws Exception {
        BM indexMask = bitmaps.buildIndexMask(lastActivityIndex, Optional.<BM>absent());

        synchronized (streamLocks.lock(streamId)) {
            BM timeMask = bitmaps.buildTimeRangeMask(timeIndex, 0L, lastActivityTimestamp);
            BM filtered = bitmaps.create();
            aggregateUtil.filter(bitmaps, schema, fieldIndex, filter, filtered, -1);

            BM result = bitmaps.create();
            bitmaps.and(result, Arrays.asList(filtered, indexMask, timeMask));
            unreadTrackingIndex.applyUnread(streamId, result);
        }
    }

    public void markAllRead(MiruStreamId streamId, long timestamp) throws Exception {
        synchronized (streamLocks.lock(streamId)) {
            BM timeMask = bitmaps.buildTimeRangeMask(timeIndex, 0L, timestamp);
            unreadTrackingIndex.applyRead(streamId, timeMask);
        }
    }
}
