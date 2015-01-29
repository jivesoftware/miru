package com.jivesoftware.os.miru.plugin.context;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.solution.MiruAggregateUtil;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import java.util.Arrays;

/** @author jonathan */
public class MiruReadTracker {

    private final MiruAggregateUtil aggregateUtil;

    public MiruReadTracker(MiruAggregateUtil aggregateUtil) {
        this.aggregateUtil = aggregateUtil;
    }

    public <BM> void read(MiruBitmaps<BM> bitmaps,
        MiruRequestContext<BM> context,
        MiruStreamId streamId,
        MiruFilter filter,
        MiruSolutionLog solutionLog,
        int lastActivityIndex,
        long lastActivityTimestamp)
        throws Exception {

        BM indexMask = bitmaps.buildIndexMask(lastActivityIndex, Optional.<BM>absent());

        synchronized (context.getStreamLocks().lock(streamId)) {
            BM timeMask = bitmaps.buildTimeRangeMask(context.getTimeIndex(), 0L, lastActivityTimestamp);
            BM filtered = bitmaps.create();
            aggregateUtil.filter(bitmaps, context.getSchema(), context.getTermComposer(), context.getFieldIndexProvider(), filter, solutionLog, filtered,
                context.getActivityIndex().lastId(), -1);

            BM result = bitmaps.create();
            bitmaps.and(result, Arrays.asList(filtered, indexMask, timeMask));
            context.getUnreadTrackingIndex().applyRead(streamId, result);
        }
    }

    public <BM> void unread(MiruBitmaps<BM> bitmaps,
        MiruRequestContext<BM> context,
        MiruStreamId streamId,
        MiruFilter filter,
        MiruSolutionLog solutionLog,
        int lastActivityIndex,
        long lastActivityTimestamp)
        throws Exception {

        BM indexMask = bitmaps.buildIndexMask(lastActivityIndex, Optional.<BM>absent());

        synchronized (context.getStreamLocks().lock(streamId)) {
            BM timeMask = bitmaps.buildTimeRangeMask(context.getTimeIndex(), 0L, lastActivityTimestamp);
            BM filtered = bitmaps.create();
            aggregateUtil.filter(bitmaps, context.getSchema(), context.getTermComposer(), context.getFieldIndexProvider(), filter, solutionLog, filtered,
                context.getActivityIndex().lastId(), -1);

            BM result = bitmaps.create();
            bitmaps.and(result, Arrays.asList(filtered, indexMask, timeMask));
            context.getUnreadTrackingIndex().applyUnread(streamId, result);
        }
    }

    public <BM> void markAllRead(MiruBitmaps<BM> bitmaps,
        MiruRequestContext<BM> context,
        MiruStreamId streamId,
        long timestamp)
        throws Exception {

        synchronized (context.getStreamLocks().lock(streamId)) {
            BM timeMask = bitmaps.buildTimeRangeMask(context.getTimeIndex(), 0L, timestamp);
            context.getUnreadTrackingIndex().applyRead(streamId, timeMask);
        }
    }
}
