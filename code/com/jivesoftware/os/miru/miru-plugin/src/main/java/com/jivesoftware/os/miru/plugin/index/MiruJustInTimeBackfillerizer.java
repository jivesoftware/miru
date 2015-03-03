package com.jivesoftware.os.miru.plugin.index;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.MiruReadEvent;
import com.jivesoftware.os.miru.api.base.MiruIBA;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.MiruIntIterator;
import com.jivesoftware.os.miru.plugin.context.MiruReadTracker;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.solution.MiruAggregateUtil;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALReader.StreamReadTrackingSipWAL;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALReader.StreamReadTrackingWAL;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/** @author jonathan */
public class MiruJustInTimeBackfillerizer {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final MiruHost localHost;
    private final Optional<String> readStreamIdsPropName;
    private final ExecutorService backfillExecutor;
    private final MiruAggregateUtil aggregateUtil = new MiruAggregateUtil();
    private final MiruReadTracker readTracker = new MiruReadTracker(aggregateUtil);

    public MiruJustInTimeBackfillerizer(MiruHost localHost, Optional<String> readStreamIdsPropName, ExecutorService backfillExecutor) {
        this.localHost = localHost;
        this.readStreamIdsPropName = readStreamIdsPropName;
        this.backfillExecutor = backfillExecutor;
    }

    public <BM> void backfill(final MiruBitmaps<BM> bitmaps,
        final MiruRequestContext<BM> requestContext,
        final MiruFilter streamFilter,
        final MiruSolutionLog solutionLog,
        final MiruTenantId tenantId,
        final MiruPartitionId partitionId,
        final MiruStreamId streamId)
        throws Exception {

        // backfill in another thread to guard WAL interface from solver cancellation/interruption
        Future<?> future = backfillExecutor.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                try {

                    synchronized (requestContext.getStreamLocks().lock(streamId, 0)) {
                        int lastActivityIndex = requestContext.getInboxIndex().getLastActivityIndex(streamId);
                        int lastId = Math.min(requestContext.getTimeIndex().lastId(), requestContext.getActivityIndex().lastId());
                        BM answer = bitmaps.create();
                        aggregateUtil.filter(bitmaps, requestContext.getSchema(), requestContext.getTermComposer(), requestContext.getFieldIndexProvider(),
                            streamFilter, solutionLog, answer, requestContext.getActivityIndex().lastId(), lastActivityIndex);

                        MiruInvertedIndexAppender inbox = requestContext.getInboxIndex().getAppender(streamId);
                        MiruInvertedIndexAppender unread = requestContext.getUnreadTrackingIndex().getAppender(streamId);
                        if (log.isDebugEnabled()) {
                            log.debug("before:\n  host={}\n  streamId={}\n  inbox={}\n  unread={}\n  last={}",
                                localHost,
                                streamId.getBytes(),
                                requestContext.getInboxIndex().getInbox(streamId).getIndex(),
                                requestContext.getUnreadTrackingIndex().getUnread(streamId).getIndex(),
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
                                MiruInternalActivity miruActivity = requestContext.getActivityIndex().get(tenantId, i);
                                if (miruActivity == null) {
                                    log.warn("Missing activity at index {}, timeIndex={}, activityIndex={}",
                                        i, requestContext.getTimeIndex().lastId(), requestContext.getActivityIndex().lastId());
                                    continue;
                                }
                                oldestBackfilledEventId = Math.min(oldestBackfilledEventId, miruActivity.time);

                                inboxIds.add(i);

                                MiruIBA[] readStreamIds = propId >= 0 ? miruActivity.propsValues[propId] : null;
                                if (readStreamIds == null || !Arrays.asList(readStreamIds).contains(streamIdAsIBA)) {
                                    unreadIds.add(i);
                                }
                            }
                        }
                        inbox.appendAndExtend(inboxIds, lastId);
                        unread.appendAndExtend(unreadIds, lastId);

                        if (log.isDebugEnabled()) {
                            log.debug("after:\n  host={}\n  streamId={}\n  inbox={}\n  unread={}\n  last={}",
                                localHost,
                                streamId.getBytes(),
                                requestContext.getInboxIndex().getInbox(streamId).getIndex(),
                                requestContext.getUnreadTrackingIndex().getUnread(streamId).getIndex(),
                                lastActivityIndex);
                        }

                        sipAndApplyReadTracking(bitmaps, requestContext, tenantId, partitionId, streamId, solutionLog, lastId, oldestBackfilledEventId);
                    }

                } catch (Exception e) {
                    log.error("Backfillerizer failed", e);
                    throw new RuntimeException("Backfillerizer failed");
                }
                return null;
            }
        });

        // if this is interrupted, the backfill will still complete
        future.get();
    }

    private <BM> void sipAndApplyReadTracking(final MiruBitmaps<BM> bitmaps,
        final MiruRequestContext<BM> requestContext,
        MiruTenantId tenantId,
        MiruPartitionId partitionId,
        MiruStreamId streamId,
        final MiruSolutionLog solutionLog,
        final int lastActivityIndex,
        long oldestBackfilledEventId) throws Exception {

        final AtomicLong minimumEventId = new AtomicLong(Long.MAX_VALUE);
        final AtomicLong newSipTimestamp = new AtomicLong(0);

        // First find the oldest eventId from our sip WAL
        long afterTimestamp = requestContext.getReadTrackingWALReader().getSipTimestamp(tenantId, partitionId, streamId);
        requestContext.getReadTrackingWALReader().streamSip(tenantId, streamId, afterTimestamp, new StreamReadTrackingSipWAL() {
            @Override
            public boolean stream(long eventId, long timestamp) throws Exception {
                long min = Math.min(minimumEventId.get(), eventId);
                minimumEventId.set(min);
                newSipTimestamp.set(timestamp);

                return true;
            }
        });

        // Take either the oldest backfilled time or the oldest readtracking sip time
        minimumEventId.set(Math.min(minimumEventId.get(), oldestBackfilledEventId));

        // If we aren't using the sip wal eventId, don't use the sip wal timestamp
        if (minimumEventId.get() == oldestBackfilledEventId) {
            newSipTimestamp.set(0);
        }

        // Take the oldest eventId from the sip WAL, grab all read/unread events from that time and apply them
        requestContext.getReadTrackingWALReader().stream(tenantId, streamId, minimumEventId.get(), new StreamReadTrackingWAL() {
            @Override
            public boolean stream(long eventId, MiruPartitionedActivity partitionedActivity, long timestamp) throws Exception {
                MiruReadEvent readEvent = partitionedActivity.readEvent.get();
                MiruStreamId streamId = readEvent.streamId;
                MiruFilter filter = readEvent.filter;

                if (partitionedActivity.type == MiruPartitionedActivity.Type.READ) {
                    readTracker.read(bitmaps, requestContext, streamId, filter, solutionLog, lastActivityIndex, readEvent.time);
                } else if (partitionedActivity.type == MiruPartitionedActivity.Type.UNREAD) {
                    readTracker.unread(bitmaps, requestContext, streamId, filter, solutionLog, lastActivityIndex, readEvent.time);
                } else if (partitionedActivity.type == MiruPartitionedActivity.Type.MARK_ALL_READ) {
                    readTracker.markAllRead(bitmaps, requestContext, streamId, readEvent.time);
                }

                return true;
            }
        });

        // Update stream sip time if we used the read tracking sip WAL
        if (newSipTimestamp.get() != 0) {
            requestContext.getReadTrackingWALReader().setSipTimestamp(tenantId, partitionId, streamId, newSipTimestamp.get());
        }

    }
}
