package com.jivesoftware.os.miru.analytics.plugins.analytics;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruValue;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmapsDebug;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.solution.MiruAggregateUtil;
import com.jivesoftware.os.miru.plugin.solution.MiruPartitionResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruRemotePartition;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruRequestHandle;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
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
        MiruRequestContext<BM, IBM, ?> context = handle.getRequestContext();
        MiruBitmaps<BM, IBM> bitmaps = handle.getBitmaps();
        StackBuffer stackBuffer = new StackBuffer();

        List<AnalyticsQueryScoreSet> scoreSets = request.query.scoreSets;
        int lastId = context.getActivityIndex().lastId(stackBuffer);

        String[] keys = new String[scoreSets.size()];
        @SuppressWarnings("unchecked")
        List<Waveform>[] waveforms = new List[scoreSets.size()];
        Analytics.AnalyticsScoreable[] scoreables = new Analytics.AnalyticsScoreable[scoreSets.size()];
        long minTimestamp = Long.MAX_VALUE;
        long maxTimestamp = Long.MIN_VALUE;

        int ssi = 0;
        for (AnalyticsQueryScoreSet scoreSet : scoreSets) {
            keys[ssi] = scoreSet.key;
            waveforms[ssi] = Lists.newArrayListWithExpectedSize(request.query.analyticsFilters.size());
            scoreables[ssi] = new Analytics.AnalyticsScoreable(scoreSet.timeRange, scoreSet.divideTimeRangeIntoNSegments);
            minTimestamp = Math.min(minTimestamp, scoreSet.timeRange.smallestTimestamp);
            maxTimestamp = Math.max(maxTimestamp, scoreSet.timeRange.largestTimestamp);
        }

        boolean resultsExhausted = analytics.analyze("analytics",
            solutionLog,
            handle,
            context,
            request.authzExpression,
            new MiruTimeRange(minTimestamp, maxTimestamp),
            request.query.constraintsFilter,
            scoreables,
            stackBuffer,
            (Analytics.ToAnalyze<MiruValue, BM> toAnalyze) -> {
                for (Map.Entry<String, MiruFilter> entry : request.query.analyticsFilters.entrySet()) {
                    BM waveformFiltered = aggregateUtil.filter("analytics", bitmaps, context, entry.getValue(), solutionLog, null, lastId, -1, stackBuffer);

                    if (!toAnalyze.analyze(new MiruValue(entry.getKey()), waveformFiltered)) {
                        return false;
                    }
                }
                return true;
            },
            (int index, MiruValue term, long[] waveformBuffer) -> {
                if (waveformBuffer == null) {
                    waveforms[index].add(Waveform.empty(term, scoreables[index].divideTimeRangeIntoNSegments));
                } else {
                    waveforms[index].add(Waveform.compressed(term, waveformBuffer));
                }
                return true;
            });

        Map<String, List<Waveform>> resultWaveforms = Maps.newHashMap();
        for (int i = 0; i < waveforms.length; i++) {
            resultWaveforms.put(keys[i], waveforms[i]);
        }
        AnalyticsAnswer result = new AnalyticsAnswer(resultWaveforms, resultsExhausted);

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
