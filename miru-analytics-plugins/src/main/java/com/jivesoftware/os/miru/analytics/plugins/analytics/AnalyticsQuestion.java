package com.jivesoftware.os.miru.analytics.plugins.analytics;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmapsDebug;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.solution.MiruAggregateUtil;
import com.jivesoftware.os.miru.plugin.solution.MiruPartitionResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruRemotePartition;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruRequestHandle;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.Question;
import com.jivesoftware.os.miru.plugin.solution.Waveform;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class AnalyticsQuestion implements Question<AnalyticsQuery, AnalyticsAnswer, AnalyticsReport> {

    private final Analytics analytics;
    private final MiruRequest<AnalyticsQuery> request;
    private final MiruRemotePartition<AnalyticsQuery, AnalyticsAnswer, AnalyticsReport> remotePartition;
    private final MiruBitmapsDebug bitmapsDebug = new MiruBitmapsDebug();
    private final MiruAggregateUtil aggregateUtil = new MiruAggregateUtil();

    public AnalyticsQuestion(Analytics analytics,
        MiruRequest<AnalyticsQuery> request,
        MiruRemotePartition<AnalyticsQuery, AnalyticsAnswer, AnalyticsReport> remotePartition) {
        this.analytics = analytics;
        this.request = request;
        this.remotePartition = remotePartition;
    }

    @Override
    public <BM extends IBM, IBM> MiruPartitionResponse<AnalyticsAnswer> askLocal(MiruRequestHandle<BM, IBM, ?> handle,
        Optional<AnalyticsReport> report) throws Exception {

        MiruSolutionLog solutionLog = new MiruSolutionLog(request.logLevel);
        MiruRequestContext<IBM, ?> context = handle.getRequestContext();
        List<Waveform> waveforms = Lists.newArrayListWithCapacity(request.query.analyticsFilters.size());
        int segments = request.query.divideTimeRangeIntoNSegments;
        boolean resultsExhausted = analytics.analyze(solutionLog,
            handle,
            context,
            request.authzExpression,
            request.query.timeRange,
            request.query.constraintsFilter, segments,
            (Analytics.ToAnalyze<String> toAnalyze) -> {
                for (Map.Entry<String, MiruFilter> entry : request.query.analyticsFilters.entrySet()) {
                    if (!toAnalyze.analyze(entry.getKey(), entry.getValue())) {
                        return false;
                    }
                }
                return true;
            },
            (String term, long[] waveformBuffer) -> {
                if (waveformBuffer == null) {
                    waveforms.add(Waveform.empty(term, segments));
                } else {
                    waveforms.add(Waveform.compressed(term, waveformBuffer));
                }
                return true;
            });

        AnalyticsAnswer result = new AnalyticsAnswer(waveforms, resultsExhausted);

        return new MiruPartitionResponse<>(result, solutionLog.asList());
    }

    @Override
    public MiruPartitionResponse<AnalyticsAnswer> askRemote(MiruHost host,
        MiruPartitionId partitionId,
        Optional<AnalyticsReport> report) throws MiruQueryServiceException {
        return remotePartition.askRemote(host, partitionId, request, report);
    }

    @Override
    public Optional<AnalyticsReport> createReport(Optional<AnalyticsAnswer> answer) {
        Optional<AnalyticsReport> report = Optional.absent();
        if (answer.isPresent()) {
            report = Optional.of(new AnalyticsReport());
        }
        return report;
    }

    @Override
    public String toString() {
        return "AnalyticsQuestion{"
            + "analytics=" + analytics
            + ", request=" + request
            + ", remotePartition=" + remotePartition
            + ", bitmapsDebug=" + bitmapsDebug
            + ", aggregateUtil=" + aggregateUtil
            + '}';
    }

}
