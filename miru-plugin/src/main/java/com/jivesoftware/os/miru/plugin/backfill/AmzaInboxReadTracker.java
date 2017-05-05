package com.jivesoftware.os.miru.plugin.backfill;

import com.google.common.collect.Maps;
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
import com.jivesoftware.os.miru.api.wal.MiruWALEntry;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.solution.MiruAggregateUtil;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/** @author jonathan */
public class AmzaInboxReadTracker implements MiruInboxReadTracker {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    // TODO - this should probably live in the context
    private final Map<MiruTenantPartitionAndStreamId, Collection<NamedCursor>> userSipTransactionId = Maps.newConcurrentMap();

    private final MiruWALClient<AmzaCursor, AmzaSipCursor> walClient;
    private final MiruAggregateUtil aggregateUtil = new MiruAggregateUtil();
    private final MiruReadTracker readTracker = new MiruReadTracker(aggregateUtil);

    public AmzaInboxReadTracker(MiruWALClient<AmzaCursor, AmzaSipCursor> walClient) {
        this.walClient = walClient;
    }

    private void setSipCursors(MiruTenantId tenantId, MiruPartitionId partitionId, MiruStreamId streamId, Collection<NamedCursor> cursors) {
        userSipTransactionId.put(new MiruTenantPartitionAndStreamId(tenantId, partitionId, streamId), cursors);
    }

    private Collection<NamedCursor> getSipCursors(MiruTenantId tenantId, MiruPartitionId partitionId, MiruStreamId streamId) {
        Collection<NamedCursor> cursors = userSipTransactionId.get(new MiruTenantPartitionAndStreamId(tenantId, partitionId, streamId));
        return cursors != null ? cursors : Collections.emptyList();
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

        Collection<NamedCursor> cursors = getSipCursors(tenantId, partitionId, streamId);
        if (verbose) {
            LOG.info("Backfill name:{} tenantId:{} partitionId:{} unread streamId:{} cursors:{}", name, tenantId, partitionId, streamId, cursors);
        }
        MiruWALClient.StreamBatch<MiruWALEntry, AmzaSipCursor> got = walClient.getRead(tenantId,
            streamId,
            new AmzaSipCursor(cursors, false),
            oldestBackfilledTimestamp,
            1000,
            true);
        AmzaSipCursor lastCursor = null;
        int calls = 1;
        int count = 0;
        while (got != null && !got.activities.isEmpty()) {
            if (verbose) {
                LOG.info("Backfill unread tenantId:{} partitionId:{} streamId:{} got:{}", tenantId, partitionId, streamId, got.activities.size());
            }
            count += got.activities.size();
            lastCursor = got.cursor;
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
            got = (got.cursor != null) ? walClient.getRead(tenantId, streamId, got.cursor, Long.MAX_VALUE, 1000, true) : null;
            calls++;
        }

        if (lastCursor != null) {
            setSipCursors(tenantId, partitionId, streamId, lastCursor.cursors);
        }
    }

    private class MiruTenantPartitionAndStreamId {

        private final MiruTenantId tenantId;
        private final MiruPartitionId partitionId;
        private final MiruStreamId streamId;

        private MiruTenantPartitionAndStreamId(MiruTenantId tenantId, MiruPartitionId partitionId, MiruStreamId streamId) {
            this.tenantId = tenantId;
            this.partitionId = partitionId;
            this.streamId = streamId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            MiruTenantPartitionAndStreamId that = (MiruTenantPartitionAndStreamId) o;

            if (partitionId != null ? !partitionId.equals(that.partitionId) : that.partitionId != null) {
                return false;
            }
            if (streamId != null ? !streamId.equals(that.streamId) : that.streamId != null) {
                return false;
            }
            return !(tenantId != null ? !tenantId.equals(that.tenantId) : that.tenantId != null);
        }

        @Override
        public int hashCode() {
            int result = tenantId != null ? tenantId.hashCode() : 0;
            result = 31 * result + (partitionId != null ? partitionId.hashCode() : 0);
            result = 31 * result + (streamId != null ? streamId.hashCode() : 0);
            return result;
        }
    }
}
