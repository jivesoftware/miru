package com.jivesoftware.os.miru.plugin.backfill;

import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.solution.MiruAggregateUtil;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import java.util.Arrays;

/** @author jonathan */
public class MiruReadTracker {

    private final MiruAggregateUtil aggregateUtil;

    public MiruReadTracker(MiruAggregateUtil aggregateUtil) {
        this.aggregateUtil = aggregateUtil;
    }

    public <BM extends IBM, IBM> void read(MiruBitmaps<BM, IBM> bitmaps,
        MiruRequestContext<BM, IBM, ?> context,
        MiruStreamId streamId,
        MiruFilter filter,
        MiruSolutionLog solutionLog,
        int lastActivityIndex,
        long lastActivityTimestamp,
        StackBuffer stackBuffer)
        throws Exception {

        IBM indexMask = bitmaps.buildIndexMask(lastActivityIndex, null, null, stackBuffer);

        synchronized (context.getStreamLocks().lock(streamId, 0)) {
            IBM timeMask = bitmaps.buildTimeRangeMask(context.getTimeIndex(), 0L, lastActivityTimestamp, stackBuffer);
            int lastId = context.getActivityIndex().lastId(stackBuffer);
            BM filtered = aggregateUtil.filter("readTrackerRead", bitmaps, context, filter, solutionLog, null, lastId, -1, stackBuffer);

            BM result = bitmaps.and(Arrays.asList(filtered, indexMask, timeMask));
            context.getUnreadTrackingIndex().applyRead(streamId, result, stackBuffer);
        }
    }

    public <BM extends IBM, IBM> void unread(MiruBitmaps<BM, IBM> bitmaps,
        MiruRequestContext<BM, IBM, ?> context,
        MiruStreamId streamId,
        MiruFilter filter,
        MiruSolutionLog solutionLog,
        int lastActivityIndex,
        long lastActivityTimestamp,
        StackBuffer stackBuffer)
        throws Exception {

        IBM indexMask = bitmaps.buildIndexMask(lastActivityIndex, null, null, stackBuffer);

        synchronized (context.getStreamLocks().lock(streamId, 0)) {
            IBM timeMask = bitmaps.buildTimeRangeMask(context.getTimeIndex(), 0L, lastActivityTimestamp, stackBuffer);
            int lastId = context.getActivityIndex().lastId(stackBuffer);
            BM filtered = aggregateUtil.filter("readTrackUnread", bitmaps, context, filter, solutionLog, null, lastId, -1, stackBuffer);

            BM result = bitmaps.and(Arrays.asList(filtered, indexMask, timeMask));
            context.getUnreadTrackingIndex().applyUnread(streamId, result, stackBuffer);
        }
    }

    public <BM extends IBM, IBM> void markAllRead(MiruBitmaps<BM, IBM> bitmaps,
        MiruRequestContext<BM, IBM, ?> context,
        MiruStreamId streamId,
        long timestamp,
        StackBuffer stackBuffer)
        throws Exception {

        synchronized (context.getStreamLocks().lock(streamId, 0)) {
            IBM timeMask = bitmaps.buildTimeRangeMask(context.getTimeIndex(), 0L, timestamp, stackBuffer);
            context.getUnreadTrackingIndex().applyRead(streamId, timeMask, stackBuffer);
        }
    }
}
