package com.jivesoftware.os.miru.plugin.backfill;

import com.google.common.base.Optional;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruIBA;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.MiruIntIterator;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.BitmapAndLastId;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndexAppender;
import com.jivesoftware.os.miru.plugin.index.TimeVersionRealtime;
import com.jivesoftware.os.miru.plugin.solution.MiruAggregateUtil;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 *
 */
public class MiruJustInTimeBackfillerizer {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final MiruInboxReadTracker inboxReadTracker;

    private final MiruHost localHost;
    private final Optional<String> readStreamIdsPropName;
    private final ExecutorService backfillExecutor;
    private final MiruAggregateUtil aggregateUtil = new MiruAggregateUtil();

    public MiruJustInTimeBackfillerizer(MiruInboxReadTracker inboxReadTracker,
        MiruHost localHost,
        Optional<String> readStreamIdsPropName,
        ExecutorService backfillExecutor) {
        this.inboxReadTracker = inboxReadTracker;
        this.localHost = localHost;
        this.readStreamIdsPropName = readStreamIdsPropName;
        this.backfillExecutor = backfillExecutor;
    }

    public <BM extends IBM, IBM> void backfillUnread(MiruBitmaps<BM, IBM> bitmaps,
        MiruRequestContext<BM, IBM, ?> requestContext,
        MiruSolutionLog solutionLog,
        MiruTenantId tenantId,
        MiruPartitionId partitionId,
        MiruStreamId streamId)
        throws Exception {

        // backfill in another thread to guard WAL interface from solver cancellation/interruption
        Future<?> future = backfillExecutor.submit(() -> {
            try {
                StackBuffer stackBuffer = new StackBuffer();
                synchronized (requestContext.getStreamLocks().lock(streamId, 0)) {
                    int lastActivityIndex = requestContext.getUnreadTrackingIndex().getLastActivityIndex(streamId, stackBuffer);
                    int lastId = requestContext.getActivityIndex().lastId(stackBuffer);

                    MiruInvertedIndexAppender unread = requestContext.getUnreadTrackingIndex().getAppender(streamId);
                    if (log.isDebugEnabled()) {
                        BitmapAndLastId<BM> container = new BitmapAndLastId<>();
                        requestContext.getUnreadTrackingIndex().getUnread(streamId).getIndex(container, stackBuffer);
                        log.debug("before:\n  host={}\n  streamId={}\n  unread={}\n  last={}",
                            localHost,
                            streamId.getBytes(),
                            container,
                            lastActivityIndex);
                    }

                    long oldestBackfilledEventId = Long.MAX_VALUE;
                    TIntList unreadIds = new TIntArrayList();
                    for (int i = lastActivityIndex + 1; i <= lastId; i++) {
                        unreadIds.add(i);
                    }
                    unread.set(stackBuffer, unreadIds.toArray());

                    if (log.isDebugEnabled()) {
                        BitmapAndLastId<BM> container = new BitmapAndLastId<>();
                        requestContext.getUnreadTrackingIndex().getUnread(streamId).getIndex(container, stackBuffer);
                        log.debug("after:\n  host={}\n  streamId={}\n  unread={}\n  last={}",
                            localHost,
                            streamId.getBytes(),
                            container,
                            lastActivityIndex);
                    }

                    inboxReadTracker.sipAndApplyReadTracking(bitmaps,
                        requestContext,
                        tenantId,
                        partitionId,
                        streamId,
                        solutionLog,
                        lastId,
                        oldestBackfilledEventId,
                        stackBuffer);
                }

            } catch (Exception e) {
                log.error("Backfillerizer failed", e);
                throw new RuntimeException("Backfillerizer failed");
            }
            return null;
        });

        // if this is interrupted, the backfill will still complete
        future.get();
    }

    public <BM extends IBM, IBM> void backfill(final MiruBitmaps<BM, IBM> bitmaps,
        final MiruRequestContext<BM, IBM, ?> requestContext,
        final MiruFilter streamFilter,
        final MiruSolutionLog solutionLog,
        final MiruTenantId tenantId,
        final MiruPartitionId partitionId,
        final MiruStreamId streamId)
        throws Exception {

        // backfill in another thread to guard WAL interface from solver cancellation/interruption
        Future<?> future = backfillExecutor.submit(() -> {
            try {
                StackBuffer stackBuffer = new StackBuffer();
                synchronized (requestContext.getStreamLocks().lock(streamId, 0)) {
                    int lastActivityIndex = requestContext.getInboxIndex().getLastActivityIndex(streamId, stackBuffer);
                    int lastId = requestContext.getActivityIndex().lastId(stackBuffer);
                    BM answer = aggregateUtil.filter("justInTimeBackfillerizer", bitmaps, requestContext, streamFilter, solutionLog, null,
                        lastId,
                        lastActivityIndex, stackBuffer);

                    MiruInvertedIndexAppender inbox = requestContext.getInboxIndex().getAppender(streamId);
                    MiruInvertedIndexAppender unread = requestContext.getUnreadTrackingIndex().getAppender(streamId);
                    if (log.isDebugEnabled()) {
                        BitmapAndLastId<BM> inboxContainer = new BitmapAndLastId<>();
                        requestContext.getInboxIndex().getInbox(streamId).getIndex(inboxContainer, stackBuffer);
                        BitmapAndLastId<BM> unreadContainer = new BitmapAndLastId<>();
                        requestContext.getUnreadTrackingIndex().getUnread(streamId).getIndex(inboxContainer, stackBuffer);
                        log.debug("before:\n  host={}\n  streamId={}\n  inbox={}\n  unread={}\n  last={}",
                            localHost,
                            streamId.getBytes(),
                            inboxContainer,
                            unreadContainer,
                            lastActivityIndex);
                    }

                    MiruIBA streamIdAsIBA = new MiruIBA(streamId.getBytes());

                    long oldestBackfilledEventId = Long.MAX_VALUE;
                    int propId = readStreamIdsPropName.isPresent() ? requestContext.getSchema().getPropertyId(readStreamIdsPropName.get()) : -1;
                    //TODO more efficient way to merge answer into inbox and unread
                    MiruIntIterator intIterator = bitmaps.intIterator(answer);
                    TIntList inboxIds = new TIntArrayList();
                    TIntList unreadIds = new TIntArrayList();
                    while (intIterator.hasNext()) {
                        int i = intIterator.next();
                        if (i > lastActivityIndex && i <= lastId) {
                            TimeVersionRealtime tvr = requestContext.getActivityIndex().getTimeVersionRealtime("justInTimeBackfillerizer", i, stackBuffer);
                            if (tvr == null) {
                                log.warn("Missing activity at index {}, timeIndex={}, activityIndex={}",
                                    i, requestContext.getTimeIndex().lastId(), lastId);
                                continue;
                            }
                            oldestBackfilledEventId = Math.min(oldestBackfilledEventId, tvr.timestamp);

                            inboxIds.add(i);

                            MiruIBA[] readStreamIds = propId < 0 ? null
                                : requestContext.getActivityIndex().getProp("justInTimeBackfillerizer", i, propId, stackBuffer);
                            if (readStreamIds == null || !Arrays.asList(readStreamIds).contains(streamIdAsIBA)) {
                                unreadIds.add(i);
                            }
                        }
                    }
                    inbox.set(stackBuffer, inboxIds.toArray());
                    unread.set(stackBuffer, unreadIds.toArray());

                    if (log.isDebugEnabled()) {
                        BitmapAndLastId<BM> inboxContainer = new BitmapAndLastId<>();
                        requestContext.getInboxIndex().getInbox(streamId).getIndex(inboxContainer, stackBuffer);
                        BitmapAndLastId<BM> unreadContainer = new BitmapAndLastId<>();
                        requestContext.getUnreadTrackingIndex().getUnread(streamId).getIndex(inboxContainer, stackBuffer);
                        log.debug("after:\n  host={}\n  streamId={}\n  inbox={}\n  unread={}\n  last={}",
                            localHost,
                            streamId.getBytes(),
                            inboxContainer,
                            unreadContainer,
                            lastActivityIndex);
                    }

                    inboxReadTracker.sipAndApplyReadTracking(bitmaps,
                        requestContext,
                        tenantId,
                        partitionId,
                        streamId,
                        solutionLog,
                        lastId,
                        oldestBackfilledEventId,
                        stackBuffer);
                }

            } catch (Exception e) {
                log.error("Backfillerizer failed", e);
                throw new RuntimeException("Backfillerizer failed");
            }
            return null;
        });

        // if this is interrupted, the backfill will still complete
        future.get();
    }
}
