package com.jivesoftware.os.miru.reco.plugins.distincts;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
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

    private final Distincts distincts;
    private final MiruRequest<DistinctsQuery> request;
    private final MiruRemotePartition<DistinctsQuery, DistinctsAnswer, DistinctsReport> remotePartition;

    public DistinctsQuestion(Distincts distincts,
        MiruRequest<DistinctsQuery> request,
        MiruRemotePartition<DistinctsQuery, DistinctsAnswer, DistinctsReport> remotePartition) {
        this.distincts = distincts;
        this.request = request;
        this.remotePartition = remotePartition;
    }

    @Override
    public <BM extends IBM, IBM> MiruPartitionResponse<DistinctsAnswer> askLocal(MiruRequestHandle<BM, IBM, ?> handle,
        Optional<DistinctsReport> report) throws Exception {

        MiruSolutionLog solutionLog = new MiruSolutionLog(request.logLevel);
        MiruRequestContext<BM, IBM, ?> context = handle.getRequestContext();
        MiruBitmaps<BM, IBM> bitmaps = handle.getBitmaps();

        //TODO config batch size
        return new MiruPartitionResponse<>(distincts.gather(bitmaps, context, request.query, 100, solutionLog), solutionLog.asList());
    }

    @Override
    public MiruPartitionResponse<DistinctsAnswer> askRemote(MiruHost host,
        MiruPartitionId partitionId,
        Optional<DistinctsReport> report) throws MiruQueryServiceException {
        return remotePartition.askRemote(host, partitionId, request, report);
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
