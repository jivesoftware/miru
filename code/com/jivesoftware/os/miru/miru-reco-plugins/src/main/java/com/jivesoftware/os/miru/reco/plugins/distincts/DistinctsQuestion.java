package com.jivesoftware.os.miru.reco.plugins.distincts;

import com.google.common.base.Optional;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmapsDebug;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.MiruTimeIndex;
import com.jivesoftware.os.miru.plugin.solution.MiruAggregateUtil;
import com.jivesoftware.os.miru.plugin.solution.MiruPartitionResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruRequestHandle;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.plugin.solution.Question;

/**
 *
 */
public class DistinctsQuestion implements Question<DistinctsAnswer, DistinctsReport> {

    private final Distincts distincts;
    private final MiruRequest<DistinctsQuery> request;
    private final MiruBitmapsDebug bitmapsDebug = new MiruBitmapsDebug();
    private final MiruAggregateUtil aggregateUtil = new MiruAggregateUtil();

    public DistinctsQuestion(Distincts distincts,
        MiruRequest<DistinctsQuery> request) {
        this.distincts = distincts;
        this.request = request;
    }

    @Override
    public <BM> MiruPartitionResponse<DistinctsAnswer> askLocal(MiruRequestHandle<BM> handle, Optional<DistinctsReport> report) throws Exception {
        MiruSolutionLog solutionLog = new MiruSolutionLog(request.debug);
        MiruRequestContext<BM> stream = handle.getRequestContext();
        MiruBitmaps<BM> bitmaps = handle.getBitmaps();

        return new MiruPartitionResponse<>(distincts.gather(bitmaps, stream, request, report, solutionLog), solutionLog.asList());

    }

    @Override
    public MiruPartitionResponse<DistinctsAnswer> askRemote(RequestHelper requestHelper, MiruPartitionId partitionId, Optional<DistinctsReport> report)
        throws Exception {
        return new DistinctsRemotePartitionReader(requestHelper).gatherDistincts(partitionId, request, report);
    }

    @Override
    public Optional<DistinctsReport> createReport(Optional<DistinctsAnswer> answer) {
        Optional<DistinctsReport> report = Optional.absent();
        if (answer.isPresent()) {
            report = Optional.of(new DistinctsReport(
                answer.get().collectedDistincts));
        }
        return report;
    }

    private boolean timeIndexIntersectsTimeRange(MiruTimeIndex timeIndex, MiruTimeRange timeRange) {
        return timeRange.smallestTimestamp <= timeIndex.getLargestTimestamp() &&
            timeRange.largestTimestamp >= timeIndex.getSmallestTimestamp();
    }
}
