package com.jivesoftware.os.miru.reco.plugins.distincts;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.solution.MiruPartitionResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruRemotePartition;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruRequestHandle;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.Question;

/**
 *
 */
public class DistinctsQuestion implements Question<DistinctsQuery, DistinctsAnswer, DistinctsReport> {

    private static final DistinctsRemotePartition REMOTE = new DistinctsRemotePartition();

    private final Distincts distincts;
    private final MiruRequest<DistinctsQuery> request;

    public DistinctsQuestion(Distincts distincts,
        MiruRequest<DistinctsQuery> request) {
        this.distincts = distincts;
        this.request = request;
    }

    @Override
    public <BM> MiruPartitionResponse<DistinctsAnswer> askLocal(MiruRequestHandle<BM> handle, Optional<DistinctsReport> report) throws Exception {
        MiruSolutionLog solutionLog = new MiruSolutionLog(request.logLevel);
        MiruRequestContext<BM> stream = handle.getRequestContext();
        MiruBitmaps<BM> bitmaps = handle.getBitmaps();

        return new MiruPartitionResponse<>(distincts.gather(bitmaps, stream, request, report, solutionLog), solutionLog.asList());
    }

    @Override
    public MiruRemotePartition<DistinctsQuery, DistinctsAnswer, DistinctsReport> getRemotePartition() {
        return REMOTE;
    }

    @Override
    public MiruRequest<DistinctsQuery> getRequest() {
        return request;
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
}
