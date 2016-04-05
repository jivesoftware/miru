package com.jivesoftware.os.miru.plugin.backfill;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.TimeAndVersion;
import com.jivesoftware.os.miru.api.base.MiruIBA;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.MiruIntIterator;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndexAppender;
import com.jivesoftware.os.miru.plugin.solution.MiruAggregateUtil;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Arrays;
import java.util.List;
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
                    int lastId = Math.min(requestContext.getTimeIndex().lastId(), requestContext.getActivityIndex().lastId(stackBuffer));
                    BM answer = aggregateUtil.filter("justInTimeBackfillerizer", bitmaps, requestContext.getSchema(), requestContext.getTermComposer(),
                        requestContext.getFieldIndexProvider(), streamFilter, solutionLog, null, requestContext.getActivityIndex().lastId(stackBuffer),
                        lastActivityIndex, stackBuffer);

                    MiruInvertedIndexAppender inbox = requestContext.getInboxIndex().getAppender(streamId);
                    MiruInvertedIndexAppender unread = requestContext.getUnreadTrackingIndex().getAppender(streamId);
                    if (log.isDebugEnabled()) {
                        log.debug("before:\n  host={}\n  streamId={}\n  inbox={}\n  unread={}\n  last={}",
                            localHost,
                            streamId.getBytes(),
                            requestContext.getInboxIndex().getInbox(streamId).getIndex(stackBuffer),
                            requestContext.getUnreadTrackingIndex().getUnread(streamId).getIndex(stackBuffer),
                            lastActivityIndex);
                    }

                    MiruIBA streamIdAsIBA = new MiruIBA(streamId.getBytes());

                    long oldestBackfilledEventId = Long.MAX_VALUE;
                    int propId = readStreamIdsPropName.isPresent() ? requestContext.getSchema().getPropertyId(readStreamIdsPropName.get()) : -1;
                    //TODO more efficient way to merge answer into inbox and unread
                    MiruIntIterator intIterator = bitmaps.intIterator(answer);
                    List<Integer> inboxIds = Lists.newLinkedList();
                    List<Integer> unreadIds = Lists.newLinkedList();
                    while (intIterator.hasNext()) {
                        int i = intIterator.next();
                        if (i > lastActivityIndex && i <= lastId) {
                            TimeAndVersion timeAndVersion = requestContext.getActivityIndex().getTimeAndVersion("justInTimeBackfillerizer", i, stackBuffer);
                            if (timeAndVersion == null) {
                                log.warn("Missing activity at index {}, timeIndex={}, activityIndex={}",
                                    i, requestContext.getTimeIndex().lastId(), requestContext.getActivityIndex().lastId(stackBuffer));
                                continue;
                            }
                            oldestBackfilledEventId = Math.min(oldestBackfilledEventId, timeAndVersion.timestamp);

                            inboxIds.add(i);

                            MiruIBA[] readStreamIds = propId < 0 ? null :
                                requestContext.getActivityIndex().getProp("justInTimeBackfillerizer", i, propId, stackBuffer);
                            if (readStreamIds == null || !Arrays.asList(readStreamIds).contains(streamIdAsIBA)) {
                                unreadIds.add(i);
                            }
                        }
                    }
                    inbox.appendAndExtend(inboxIds, lastId, stackBuffer);
                    unread.appendAndExtend(unreadIds, lastId, stackBuffer);

                    if (log.isDebugEnabled()) {
                        log.debug("after:\n  host={}\n  streamId={}\n  inbox={}\n  unread={}\n  last={}",
                            localHost,
                            streamId.getBytes(),
                            requestContext.getInboxIndex().getInbox(streamId).getIndex(stackBuffer),
                            requestContext.getUnreadTrackingIndex().getUnread(streamId).getIndex(stackBuffer),
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
