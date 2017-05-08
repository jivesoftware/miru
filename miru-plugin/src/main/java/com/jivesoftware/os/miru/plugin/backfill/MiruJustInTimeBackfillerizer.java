package com.jivesoftware.os.miru.plugin.backfill;

import com.google.common.base.Optional;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruIBA;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.plugin.backfill.MiruInboxReadTracker.ApplyResult;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.MiruIntIterator;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.BitmapAndLastId;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndexAppender;
import com.jivesoftware.os.miru.plugin.index.TimeVersionRealtime;
import com.jivesoftware.os.miru.plugin.solution.MiruAggregateUtil;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.health.api.HealthFactory;
import com.jivesoftware.os.routing.bird.health.api.HealthTimer;
import com.jivesoftware.os.routing.bird.health.api.TimerHealthCheckConfig;
import com.jivesoftware.os.routing.bird.health.checkers.TimerHealthChecker;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.merlin.config.defaults.DoubleDefault;
import org.merlin.config.defaults.StringDefault;

/**
 *
 */
public class MiruJustInTimeBackfillerizer {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    public interface BackfillUnreadLatency extends TimerHealthCheckConfig {

        @StringDefault("backfillUnread>latency")
        @Override
        String getName();

        @StringDefault("How long its taking to backfillUnread.")
        @Override
        String getDescription();

        @DoubleDefault(30000d) /// 30sec
        @Override
        Double get95ThPecentileMax();
    }

    private static final HealthTimer backfillUnreadLatency = HealthFactory.getHealthTimer(BackfillUnreadLatency.class, TimerHealthChecker.FACTORY);

    private final MiruInboxReadTracker inboxReadTracker;

    private final Optional<String> readStreamIdsPropName;
    private final ExecutorService backfillExecutor;
    private final MiruAggregateUtil aggregateUtil = new MiruAggregateUtil();
    private final Set<MiruStreamId> verboseStreamIds;
    private final boolean verboseAllStreamIds;

    public MiruJustInTimeBackfillerizer(MiruInboxReadTracker inboxReadTracker,
        Optional<String> readStreamIdsPropName,
        ExecutorService backfillExecutor,
        Set<MiruStreamId> verboseStreamIds,
        boolean verboseAllStreamIds) {
        this.inboxReadTracker = inboxReadTracker;
        this.readStreamIdsPropName = readStreamIdsPropName;
        this.backfillExecutor = backfillExecutor;
        this.verboseStreamIds = verboseStreamIds;
        this.verboseAllStreamIds = verboseAllStreamIds;
    }

    public <BM extends IBM, IBM> void resetUnread(MiruRequestContext<BM, IBM, ?> requestContext,
        MiruStreamId streamId) throws Exception {

        synchronized (requestContext.getStreamLocks().lock(streamId, 0)) {
            requestContext.getUnreadTrackingIndex().setCursors(streamId, Collections.emptyList());
            requestContext.getUnreadTrackingIndex().setLastActivityIndex(streamId, -1, new StackBuffer());
        }
    }

    public <BM extends IBM, IBM> void backfillUnread(String name,
        MiruBitmaps<BM, IBM> bitmaps,
        MiruRequestContext<BM, IBM, ?> requestContext,
        MiruSolutionLog solutionLog,
        MiruTenantId tenantId,
        MiruPartitionId partitionId,
        MiruStreamId streamId,
        MiruFilter suppressUnreadFilter)
        throws Exception {

        LOG.inc("backfillUnread>calls>" + name);
        // backfill in another thread to guard WAL interface from solver cancellation/interruption
        MiruSolutionLog backfillSolutionLog = new MiruSolutionLog(solutionLog.getLevel());
        Future<?> future = backfillExecutor.submit(() -> {
            backfillUnreadLatency.startTimer();
            try {
                StackBuffer stackBuffer = new StackBuffer();
                synchronized (requestContext.getStreamLocks().lock(streamId, 0)) {
                    long start = System.currentTimeMillis();
                    boolean verbose = verboseAllStreamIds || verboseStreamIds != null && verboseStreamIds.contains(streamId);
                    int lastActivityIndex = requestContext.getUnreadTrackingIndex().getLastActivityIndex(streamId, stackBuffer);
                    int lastId = requestContext.getActivityIndex().lastId(stackBuffer);

                    long beforeCardinality = Long.MIN_VALUE;
                    if (verbose) {
                        BitmapAndLastId<BM> container = new BitmapAndLastId<>();
                        requestContext.getUnreadTrackingIndex().getUnread(streamId).getIndex(container, stackBuffer);
                        beforeCardinality = bitmaps.cardinality(container.getBitmap());
                    }

                    long smallestTimestamp = requestContext.getTimeIndex().getSmallestTimestamp();
                    long oldestBackfilledTimestamp = Long.MAX_VALUE;
                    TIntList unreadIds = new TIntArrayList();
                    for (int i = lastActivityIndex + 1; i <= lastId; i++) {
                        unreadIds.add(i);
                        if (oldestBackfilledTimestamp == Long.MAX_VALUE) {
                            TimeVersionRealtime tvr = requestContext.getActivityIndex().getTimeVersionRealtime("backfillUnread", i, stackBuffer);
                            if (tvr != null) {
                                oldestBackfilledTimestamp = tvr.monoTimestamp;
                            }
                        }
                    }
                    LOG.inc("backfillUnread>ids>pow>" + FilerIO.chunkPower(unreadIds.size(), 0));
                    long elapsed = System.currentTimeMillis() - start;
                    backfillSolutionLog.log(MiruSolutionLogLevel.INFO, "Got oldest backfilled timestamp in {} ms", elapsed);
                    start = System.currentTimeMillis();

                    BM unreadMask = bitmaps.createWithBits(unreadIds.toArray());
                    if (!MiruFilter.NO_FILTER.equals(suppressUnreadFilter)) {
                        BM suppressUnreadBitmap = aggregateUtil.filter("backfillUnread",
                            bitmaps,
                            requestContext,
                            suppressUnreadFilter,
                            backfillSolutionLog,
                            null,
                            lastId,
                            lastActivityIndex,
                            -1,
                            stackBuffer);
                        bitmaps.inPlaceAndNot(unreadMask, suppressUnreadBitmap);
                    }
                    requestContext.getUnreadTrackingIndex().applyUnread(streamId, unreadMask, stackBuffer);

                    long unreadMaskCardinality = Long.MIN_VALUE;
                    if (verbose) {
                        unreadMaskCardinality = bitmaps.cardinality(unreadMask);
                    }

                    elapsed = System.currentTimeMillis() - start;
                    backfillSolutionLog.log(MiruSolutionLogLevel.INFO, "Applied unread in {} ms", elapsed);
                    start = System.currentTimeMillis();

                    long middleCardinality = Long.MIN_VALUE;
                    if (verbose) {
                        BitmapAndLastId<BM> container = new BitmapAndLastId<>();
                        requestContext.getUnreadTrackingIndex().getUnread(streamId).getIndex(container, stackBuffer);
                        middleCardinality = bitmaps.cardinality(container.getBitmap());
                    }

                    ApplyResult ar = inboxReadTracker.sipAndApplyReadTracking(name,
                        bitmaps,
                        requestContext,
                        tenantId,
                        partitionId,
                        streamId,
                        backfillSolutionLog,
                        lastId,
                        smallestTimestamp,
                        oldestBackfilledTimestamp,
                        stackBuffer);

                    long afterCardinality = Long.MIN_VALUE;
                    if (verbose) {
                        BitmapAndLastId<BM> container = new BitmapAndLastId<>();
                        requestContext.getUnreadTrackingIndex().getUnread(streamId).getIndex(container, stackBuffer);
                        afterCardinality = bitmaps.cardinality(container.getBitmap());
                    }

                    if (verbose) {
                        LOG.info(
                            "Backfill unread name:{} tenantId:{} partitionId:{} streamId:{} before:{} middle:{} after:{}" +
                                " / fill mask:{} from:{} to:{} oldest:{}" +
                                " / applied calls:{} count:{} read:{} readTime:{} unread:{} unreadTime:{} allRead:{} allReadTime:{}" +
                                " / cursors init:{} applied:{}",
                            name, tenantId, partitionId, streamId, beforeCardinality, middleCardinality, afterCardinality,
                            unreadMaskCardinality, lastActivityIndex, lastId, oldestBackfilledTimestamp,
                            ar.calls, ar.count, ar.numRead, ar.maxReadTime, ar.numUnread, ar.maxUnreadTime, ar.numAllRead, ar.maxAllReadTime,
                            ar.initialCursors, ar.appliedCursors);
                    }
                    requestContext.getUnreadTrackingIndex().setLastActivityIndex(streamId, lastId, stackBuffer);

                    elapsed = System.currentTimeMillis() - start;
                    backfillSolutionLog.log(MiruSolutionLogLevel.INFO, "Applied read tracking in {} ms", elapsed);
                }

            } catch (Exception e) {
                LOG.error("Backfillerizer failed", e);
                throw new RuntimeException("Backfillerizer failed");
            } finally {
                backfillUnreadLatency.stopTimer("Backfill unread latency", "Fix indexing or downstream WAL issues");
            }
            return null;
        });

        // if this is interrupted, the backfill will still complete
        future.get();
        solutionLog.append(backfillSolutionLog);
    }

    public <BM extends IBM, IBM> void backfillInboxUnread(String name,
        final MiruBitmaps<BM, IBM> bitmaps,
        final MiruRequestContext<BM, IBM, ?> requestContext,
        final MiruFilter streamFilter,
        final MiruSolutionLog solutionLog,
        final MiruTenantId tenantId,
        final MiruPartitionId partitionId,
        final MiruStreamId streamId,
        MiruFilter suppressUnreadFilter)
        throws Exception {

        LOG.inc("backfillInboxUnread>calls>" + name);
        // backfill in another thread to guard WAL interface from solver cancellation/interruption
        MiruSolutionLog backfillSolutionLog = new MiruSolutionLog(solutionLog.getLevel());
        Future<?> future = backfillExecutor.submit(() -> {
            try {
                StackBuffer stackBuffer = new StackBuffer();
                synchronized (requestContext.getStreamLocks().lock(streamId, 0)) {
                    int lastActivityIndex = requestContext.getInboxIndex().getLastActivityIndex(streamId, stackBuffer);
                    int lastId = requestContext.getActivityIndex().lastId(stackBuffer);
                    BM answer = aggregateUtil.filter("justInTimeBackfillerizer",
                        bitmaps,
                        requestContext,
                        streamFilter,
                        backfillSolutionLog,
                        null,
                        lastId,
                        lastActivityIndex,
                        -1,
                        stackBuffer);

                    MiruInvertedIndexAppender inbox = requestContext.getInboxIndex().getAppender(streamId);
                    if (LOG.isDebugEnabled()) {
                        BitmapAndLastId<BM> inboxContainer = new BitmapAndLastId<>();
                        requestContext.getInboxIndex().getInbox(streamId).getIndex(inboxContainer, stackBuffer);
                        BitmapAndLastId<BM> unreadContainer = new BitmapAndLastId<>();
                        requestContext.getUnreadTrackingIndex().getUnread(streamId).getIndex(inboxContainer, stackBuffer);
                        LOG.debug("before:\n  streamId={}\n  inbox={}\n  unread={}\n  last={}",
                            streamId.getBytes(),
                            inboxContainer,
                            unreadContainer,
                            lastActivityIndex);
                    }

                    MiruIBA streamIdAsIBA = new MiruIBA(streamId.getBytes());

                    long smallestTimestamp = requestContext.getTimeIndex().getSmallestTimestamp();
                    long oldestBackfilledTimestamp = Long.MAX_VALUE;
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
                                LOG.warn("Missing activity at index {}, timeIndex={}, activityIndex={}",
                                    i, requestContext.getTimeIndex().lastId(), lastId);
                                continue;
                            }
                            oldestBackfilledTimestamp = Math.min(oldestBackfilledTimestamp, tvr.timestamp);

                            inboxIds.add(i);

                            MiruIBA[] readStreamIds = propId < 0 ? null
                                : requestContext.getActivityIndex().getProp("justInTimeBackfillerizer", i, propId, stackBuffer);
                            if (readStreamIds == null || !Arrays.asList(readStreamIds).contains(streamIdAsIBA)) {
                                unreadIds.add(i);
                            }
                        }
                    }
                    inbox.set(stackBuffer, inboxIds.toArray());

                    BM unreadMask = bitmaps.createWithBits(unreadIds.toArray());
                    if (!MiruFilter.NO_FILTER.equals(suppressUnreadFilter)) {
                        BM suppressUnreadBitmap = aggregateUtil.filter("backfillUnread",
                            bitmaps,
                            requestContext,
                            suppressUnreadFilter,
                            backfillSolutionLog,
                            null,
                            lastId,
                            lastActivityIndex,
                            -1,
                            stackBuffer);
                        bitmaps.inPlaceAndNot(unreadMask, suppressUnreadBitmap);
                    }
                    requestContext.getUnreadTrackingIndex().applyUnread(streamId, unreadMask, stackBuffer);

                    if (LOG.isDebugEnabled()) {
                        BitmapAndLastId<BM> inboxContainer = new BitmapAndLastId<>();
                        requestContext.getInboxIndex().getInbox(streamId).getIndex(inboxContainer, stackBuffer);
                        BitmapAndLastId<BM> unreadContainer = new BitmapAndLastId<>();
                        requestContext.getUnreadTrackingIndex().getUnread(streamId).getIndex(inboxContainer, stackBuffer);
                        LOG.debug("after:\n  streamId={}\n  inbox={}\n  unread={}\n  last={}",
                            streamId.getBytes(),
                            inboxContainer,
                            unreadContainer,
                            lastActivityIndex);
                    }

                    inboxReadTracker.sipAndApplyReadTracking(name,
                        bitmaps,
                        requestContext,
                        tenantId,
                        partitionId,
                        streamId,
                        backfillSolutionLog,
                        lastId,
                        smallestTimestamp,
                        oldestBackfilledTimestamp,
                        stackBuffer);
                }

            } catch (Exception e) {
                LOG.error("Backfillerizer failed", e);
                throw new RuntimeException("Backfillerizer failed");
            }
            return null;
        });

        // if this is interrupted, the backfill will still complete
        future.get();
        solutionLog.append(backfillSolutionLog);
    }
}
