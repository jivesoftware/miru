package com.jivesoftware.os.miru.reco.plugins.distincts;

import com.google.common.base.Optional;
import com.jivesoftware.os.jive.utils.http.client.HttpClient;
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
    public <BM> MiruPartitionResponse<DistinctsAnswer> askLocal(MiruRequestHandle<BM, ?> handle, Optional<DistinctsReport> report) throws Exception {
        MiruSolutionLog solutionLog = new MiruSolutionLog(request.logLevel);
        MiruRequestContext<BM, ?> context = handle.getRequestContext();
        MiruBitmaps<BM> bitmaps = handle.getBitmaps();

        return new MiruPartitionResponse<>(distincts.gather(bitmaps, context, request.query, solutionLog), solutionLog.asList());
    }

    @Override
    public MiruPartitionResponse<DistinctsAnswer> askRemote(HttpClient httpClient,
        MiruPartitionId partitionId,
        Optional<DistinctsReport> report) throws MiruQueryServiceException {
        return remotePartition.askRemote(httpClient, partitionId, request, report);
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
