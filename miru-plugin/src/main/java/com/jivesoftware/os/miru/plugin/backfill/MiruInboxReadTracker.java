package com.jivesoftware.os.miru.plugin.backfill;

import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;

/** @author jonathan */
public interface MiruInboxReadTracker {

    <BM> void sipAndApplyReadTracking(final MiruBitmaps<BM> bitmaps,
        final MiruRequestContext<BM, ?> requestContext,
        MiruTenantId tenantId,
        MiruPartitionId partitionId,
        MiruStreamId streamId,
        final MiruSolutionLog solutionLog,
        final int lastActivityIndex,
        long oldestBackfilledEventId) throws Exception;
}
