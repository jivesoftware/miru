package com.jivesoftware.os.miru.plugin.backfill;

import com.google.common.collect.Maps;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity.Type;
import com.jivesoftware.os.miru.api.activity.MiruReadEvent;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.miru.api.wal.MiruWALClient.OldestReadResult;
import com.jivesoftware.os.miru.api.wal.MiruWALClient.StreamBatch;
import com.jivesoftware.os.miru.api.wal.MiruWALEntry;
import com.jivesoftware.os.miru.api.wal.RCVSCursor;
import com.jivesoftware.os.miru.api.wal.RCVSSipCursor;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.solution.MiruAggregateUtil;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Map;

/** @author jonathan */
public class RCVSInboxReadTracker implements MiruInboxReadTracker {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    // TODO - this should probably live in the context
    private final Map<MiruTenantPartitionAndStreamId, Long> userSipTimestamp = Maps.newConcurrentMap();

    private final MiruWALClient<RCVSCursor, RCVSSipCursor> walClient;
    private final MiruAggregateUtil aggregateUtil = new MiruAggregateUtil();
    private final MiruReadTracker readTracker = new MiruReadTracker(aggregateUtil);

    public RCVSInboxReadTracker(MiruWALClient<RCVSCursor, RCVSSipCursor> walClient) {
        this.walClient = walClient;
    }

    private void setSipTimestamp(MiruTenantId tenantId, MiruPartitionId partitionId, MiruStreamId streamId, long sipTimestamp) {
        userSipTimestamp.put(new MiruTenantPartitionAndStreamId(tenantId, partitionId, streamId), sipTimestamp);
    }

    private long getSipTimestamp(MiruTenantId tenantId, MiruPartitionId partitionId, MiruStreamId streamId) {
        Long sipTimestamp = userSipTimestamp.get(new MiruTenantPartitionAndStreamId(tenantId, partitionId, streamId));
        return sipTimestamp != null ? sipTimestamp : 0;
    }

    @Override
    public <BM extends IBM, IBM> ApplyResult sipAndApplyReadTracking(String name,
        final MiruBitmaps<BM, IBM> bitmaps,
        final MiruRequestContext<BM, IBM, ?> requestContext,
        MiruTenantId tenantId,
        MiruPartitionId partitionId,
        MiruStreamId streamId,
        MiruSolutionLog solutionLog,
        int lastActivityIndex,
        long smallestTimestamp,
        long oldestBackfilledTimestamp,
        StackBuffer stackBuffer) throws Exception {

        // First find the oldest eventId from our sip WAL
        long afterTimestamp = getSipTimestamp(tenantId, partitionId, streamId);
        // TODO this should really be computed on the server side.
        OldestReadResult<RCVSSipCursor> oldestReadResult = walClient.oldestReadEventId(tenantId,
            streamId,
            new RCVSSipCursor(Type.ACTIVITY.getSort(), afterTimestamp, 0, false),
            true);
        RCVSSipCursor lastCursor = oldestReadResult.cursor;

        long fromTimestamp = Math.min(oldestBackfilledTimestamp, oldestReadResult.oldestEventId);
        StreamBatch<MiruWALEntry, Long> got = walClient.scanRead(tenantId, streamId, fromTimestamp, 10_000, true);
        while (got != null && !got.activities.isEmpty()) {
            for (MiruWALEntry e : got.activities) {
                MiruReadEvent readEvent = e.activity.readEvent.get();
                MiruFilter filter = readEvent.filter;

                if (e.activity.type == MiruPartitionedActivity.Type.READ) {
                    readTracker.read(bitmaps, requestContext, streamId, filter, solutionLog, lastActivityIndex, readEvent.time, stackBuffer);
                } else if (e.activity.type == MiruPartitionedActivity.Type.UNREAD) {
                    readTracker.unread(bitmaps, requestContext, streamId, filter, solutionLog, lastActivityIndex, readEvent.time, stackBuffer);
                } else if (e.activity.type == MiruPartitionedActivity.Type.MARK_ALL_READ) {
                    readTracker.markAllRead(bitmaps, requestContext, streamId, readEvent.time, stackBuffer);
                }
            }
            got = (got.cursor != null) ? walClient.scanRead(tenantId, streamId, got.cursor, 10_000, true) : null;
        }

        if (lastCursor != null) {
            setSipTimestamp(tenantId, partitionId, streamId, lastCursor.clockTimestamp);
        }

        return new ApplyResult(-1, -1, -1, -1, -1, -1, -1, -1, null, null);
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
