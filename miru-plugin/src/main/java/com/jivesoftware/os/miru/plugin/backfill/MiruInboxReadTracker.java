package com.jivesoftware.os.miru.plugin.backfill;

import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.topology.NamedCursor;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import java.util.List;

/** @author jonathan */
public interface MiruInboxReadTracker {

    <BM extends IBM, IBM> ApplyResult sipAndApplyReadTracking(String name,
        final MiruBitmaps<BM, IBM> bitmaps,
        final MiruRequestContext<BM, IBM, ?> requestContext,
        MiruTenantId tenantId,
        MiruPartitionId partitionId,
        MiruStreamId streamId,
        MiruSolutionLog solutionLog,
        int lastActivityIndex,
        long smallestTimestamp,
        long oldestBackfilledTimestamp,
        StackBuffer stackBuffer) throws Exception;

    class ApplyResult {
        public final long calls;
        public final long count;
        public final long numRead;
        public final long maxReadTime;
        public final long numUnread;
        public final long maxUnreadTime;
        public final long numAllRead;
        public final long maxAllReadTime;
        public final List<NamedCursor> initialCursors;
        public final List<NamedCursor> appliedCursors;

        public ApplyResult(long calls,
            long count,
            long numRead,
            long maxReadTime,
            long numUnread,
            long maxUnreadTime,
            long numAllRead,
            long maxAllReadTime,
            List<NamedCursor> initialCursors,
            List<NamedCursor> appliedCursors) {
            this.calls = calls;
            this.count = count;
            this.numRead = numRead;
            this.maxReadTime = maxReadTime;
            this.numUnread = numUnread;
            this.maxUnreadTime = maxUnreadTime;
            this.numAllRead = numAllRead;
            this.maxAllReadTime = maxAllReadTime;
            this.initialCursors = initialCursors;
            this.appliedCursors = appliedCursors;
        }
    }
}
