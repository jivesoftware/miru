package com.jivesoftware.os.miru.plugin.backfill;

import com.google.common.collect.Maps;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.MiruReadEvent;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.topology.NamedCursor;
import com.jivesoftware.os.miru.api.wal.AmzaCursor;
import com.jivesoftware.os.miru.api.wal.AmzaSipCursor;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.miru.api.wal.MiruWALClient.OldestReadResult;
import com.jivesoftware.os.miru.api.wal.MiruWALClient.StreamBatch;
import com.jivesoftware.os.miru.api.wal.MiruWALEntry;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.MiruSipIndex;
import com.jivesoftware.os.miru.plugin.index.MiruUnreadTrackingIndex;
import com.jivesoftware.os.miru.plugin.solution.MiruAggregateUtil;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** @author jonathan */
public class AmzaInboxReadTracker implements MiruInboxReadTracker {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruWALClient<AmzaCursor, AmzaSipCursor> walClient;
    private final MiruAggregateUtil aggregateUtil = new MiruAggregateUtil();
    private final MiruReadTracker readTracker = new MiruReadTracker(aggregateUtil);

    public AmzaInboxReadTracker(MiruWALClient<AmzaCursor, AmzaSipCursor> walClient) {
        this.walClient = walClient;
    }

    @Override
    public <BM extends IBM, IBM> void sipAndApplyReadTracking(String name,
        final MiruBitmaps<BM, IBM> bitmaps,
        final MiruRequestContext<BM, IBM, ?> requestContext,
        MiruTenantId tenantId,
        MiruPartitionId partitionId,
        MiruStreamId streamId,
        MiruSolutionLog solutionLog,
        int lastActivityIndex,
        long oldestBackfilledTimestamp,
        boolean verbose,
        StackBuffer stackBuffer) throws Exception {

        MiruUnreadTrackingIndex<BM, IBM> unreadTrackingIndex = requestContext.getUnreadTrackingIndex();
        List<NamedCursor> cursors = unreadTrackingIndex.getCursors(streamId);
        if (verbose) {
            LOG.info("Backfill name:{} tenantId:{} partitionId:{} unread streamId:{} cursors:{}", name, tenantId, partitionId, streamId, cursors);
        }
        OldestReadResult<AmzaSipCursor> oldestReadResult = walClient.oldestReadEventId(tenantId,
            streamId,
            new AmzaSipCursor(cursors, false),
            true);
        AmzaSipCursor lastCursor = oldestReadResult.cursor;

        long fromTimestamp = oldestReadResult.oldestEventId == -1 ? oldestBackfilledTimestamp
            : Math.min(oldestBackfilledTimestamp, oldestReadResult.oldestEventId);

        StreamBatch<MiruWALEntry, Long> got = (fromTimestamp == Long.MAX_VALUE) ? null : walClient.scanRead(tenantId, streamId, fromTimestamp, 10_000, true);
        int calls = 0;
        int count = 0;
        while (got != null && !got.activities.isEmpty()) {
            calls++;
            count += got.activities.size();
            if (verbose) {
                LOG.info("Backfill unread name:{} tenantId:{} partitionId:{} streamId:{} got:{}",
                    name, tenantId, partitionId, streamId, got.activities.size());
            }
            for (MiruWALEntry e : got.activities) {
                MiruReadEvent readEvent = e.activity.readEvent.get();
                MiruFilter filter = readEvent.filter;

                if (e.activity.type == MiruPartitionedActivity.Type.READ) {
                    if (verbose) {
                        LOG.info("Backfill unread name:{} tenantId:{} partitionId:{} streamId:{} read:{}",
                            name, tenantId, partitionId, streamId, readEvent.time);
                    }
                    readTracker.read(bitmaps, requestContext, streamId, filter, solutionLog, lastActivityIndex, readEvent.time, stackBuffer);
                } else if (e.activity.type == MiruPartitionedActivity.Type.UNREAD) {
                    if (verbose) {
                        LOG.info("Backfill unread name:{} tenantId:{} partitionId:{} streamId:{} unread:{}",
                            name, tenantId, partitionId, streamId, readEvent.time);
                    }
                    readTracker.unread(bitmaps, requestContext, streamId, filter, solutionLog, lastActivityIndex, readEvent.time, stackBuffer);
                } else if (e.activity.type == MiruPartitionedActivity.Type.MARK_ALL_READ) {
                    if (verbose) {
                        LOG.info("Backfill unread name:{} tenantId:{} partitionId:{} streamId:{} markAllRead:{}",
                            name, tenantId, partitionId, streamId, readEvent.time);
                    }
                    readTracker.markAllRead(bitmaps, requestContext, streamId, readEvent.time, stackBuffer);
                }
            }
            got = (got.cursor != null) ? walClient.scanRead(tenantId, streamId, got.cursor, 10_000, true) : null;
        }

        LOG.inc("sipAndApply>calls>pow>" + FilerIO.chunkPower(calls, 0));
        LOG.inc("sipAndApply>count>pow>" + FilerIO.chunkPower(count, 0));
        if (lastCursor != null) {
            unreadTrackingIndex.setCursors(streamId, lastCursor.cursors);
        }
    }
}
