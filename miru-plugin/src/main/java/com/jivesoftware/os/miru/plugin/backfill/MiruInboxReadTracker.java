package com.jivesoftware.os.miru.plugin.backfill;

import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;

/** @author jonathan */
public interface MiruInboxReadTracker {

    <BM extends IBM, IBM> void sipAndApplyReadTracking(final MiruBitmaps<BM, IBM> bitmaps,
        final MiruRequestContext<BM, IBM, ?> requestContext,
        MiruTenantId tenantId,
        MiruPartitionId partitionId,
        MiruStreamId streamId,
        MiruSolutionLog solutionLog,
        int lastActivityIndex,
        long oldestBackfilledEventId,
        StackBuffer stackBuffer) throws Exception;
}
