package com.jivesoftware.os.miru.reco.plugins.trending;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.jivesoftware.os.miru.analytics.plugins.analytics.Analytics;
import com.jivesoftware.os.miru.analytics.plugins.analytics.AnalyticsAnswer;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.query.filter.MiruFieldFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilterOperation;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.MiruTermComposer;
import com.jivesoftware.os.miru.plugin.solution.MiruPartitionResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruRemotePartition;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruRequestHandle;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.plugin.solution.Question;
import com.jivesoftware.os.miru.plugin.solution.Waveform;
import com.jivesoftware.os.miru.reco.plugins.distincts.Distincts;
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsQuery;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class TrendingQuestion implements Question<TrendingQuery, AnalyticsAnswer, TrendingReport> {

    private final Distincts distincts;
    private final Analytics analytics;
    private final MiruRequest<TrendingQuery> request;
    private final MiruTimeRange combinedTimeRange;
    private final MiruRemotePartition<TrendingQuery, AnalyticsAnswer, TrendingReport> remotePartition;

    public TrendingQuestion(Distincts distincts,
        Analytics analytics,
        MiruTimeRange combinedTimeRange,
        MiruRequest<TrendingQuery> request,
        MiruRemotePartition<TrendingQuery, AnalyticsAnswer, TrendingReport> remotePartition) {
        this.distincts = distincts;
        this.analytics = analytics;
        this.combinedTimeRange = combinedTimeRange;
        this.request = request;
        this.remotePartition = remotePartition;
    }

    @Override
    public <BM> MiruPartitionResponse<AnalyticsAnswer> askLocal(MiruRequestHandle<BM, ?> handle, Optional<TrendingReport> report) throws Exception {
        MiruSolutionLog solutionLog = new MiruSolutionLog(request.logLevel);
        MiruRequestContext<BM, ? extends MiruSipCursor<?>> context = handle.getRequestContext();

        Map<String, Waveform> waveforms = Maps.newHashMap();
        MiruTermComposer termComposer = context.getTermComposer();

        MiruSchema schema = context.getSchema();
        int fieldId = schema.getFieldId(request.query.aggregateCountAroundField);
        MiruFieldDefinition fieldDefinition = schema.getFieldDefinition(fieldId);

        boolean resultsExhausted = analytics.analyze(solutionLog,
            handle,
            context,
            request.authzExpression,
            combinedTimeRange,
            request.query.constraintsFilter,
            request.query.divideTimeRangeIntoNSegments,
            (Analytics.ToAnalyze<MiruTermId> toAnalyze) -> {
                if (request.query.distinctQueries.size() == 1) {
                    distincts.gatherDirect(handle.getBitmaps(), handle.getRequestContext(), request.query.distinctQueries.get(0), solutionLog, termId -> {
                        toAnalyze.analyze(termId, new MiruFilter(MiruFilterOperation.and,
                            false,
                            Collections.singletonList(MiruFieldFilter.raw(
                                MiruFieldType.primary, request.query.aggregateCountAroundField, Collections.singletonList(termId))),
                            null));
                        return true;
                    });
                } else if (request.query.distinctQueries.size() > 1) {
                    Set<MiruTermId> joinTerms = null;
                    for (DistinctsQuery distinctQuery : request.query.distinctQueries) {
                        Set<MiruTermId> queryTerms = Sets.newHashSet();
                        distincts.gatherDirect(handle.getBitmaps(), handle.getRequestContext(), distinctQuery, solutionLog, termId -> {
                            queryTerms.add(termId);
                            return true;
                        });
                        if (joinTerms == null) {
                            joinTerms = queryTerms;
                        } else {
                            joinTerms.retainAll(queryTerms);
                        }
                    }
                    if (joinTerms != null) {
                        for (MiruTermId termId : joinTerms) {
                            toAnalyze.analyze(termId, new MiruFilter(MiruFilterOperation.and,
                                false,
                                Collections.singletonList(MiruFieldFilter.raw(
                                    MiruFieldType.primary, request.query.aggregateCountAroundField, Collections.singletonList(termId))),
                                null));
                        }
                    }
                }
                return true;
            },
            (MiruTermId term, Waveform waveform) -> {
                waveforms.put(termComposer.decompose(fieldDefinition, term), waveform);
                return true;
            });

        AnalyticsAnswer result = new AnalyticsAnswer(waveforms, resultsExhausted);
        return new MiruPartitionResponse<>(result, solutionLog.asList());
    }

    @Override
    public MiruPartitionResponse<AnalyticsAnswer> askRemote(MiruHost host,
        MiruPartitionId partitionId,
        Optional<TrendingReport> report) throws MiruQueryServiceException {
        return remotePartition.askRemote(host, partitionId, request, report);
    }

    @Override
    public Optional<TrendingReport> createReport(Optional<AnalyticsAnswer> answer) {
        Optional<TrendingReport> report = Optional.absent();
        if (answer.isPresent()) {
            report = Optional.of(new TrendingReport());
        }
        return report;
    }
}
