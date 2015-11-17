package com.jivesoftware.os.miru.reco.plugins.trending;

import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.jivesoftware.os.miru.analytics.plugins.analytics.Analytics;
import com.jivesoftware.os.miru.analytics.plugins.analytics.AnalyticsAnswer;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
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
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.plugin.solution.Question;
import com.jivesoftware.os.miru.plugin.solution.Waveform;
import com.jivesoftware.os.miru.reco.plugins.distincts.Distincts;
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsQuery;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
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

    /*private final Map<MiruPartitionCoord, Map<String, Map<WaveformKey, Cache<String, Waveform>>>> cache;

    private static class WaveformKey {

        private final long ceilingTime;
        private final long floorTime;
        private final int numSegments;
        private final MiruFilter constraintsFiler;

    }

    private MiruPartitionResponse<AnalyticsAnswer> getCachedResponse(MiruTimeRange timeRange, int numSegments) {
        long upperTime = timeRange.largestTimestamp;
        long lowerTime = timeRange.smallestTimestamp;
        if (upperTime == Long.MAX_VALUE || lowerTime == 0) {
            return null;
        }

        long timePerSegment = (upperTime - lowerTime) / numSegments;

        long jiveModulusTime = upperTime % timePerSegment;
        long jiveCeilingTime = upperTime - jiveModulusTime + timePerSegment;
        long jiveFloorTime = jiveCeilingTime - (numSegments * timePerSegment);

    }*/

    @Override
    public <BM> MiruPartitionResponse<AnalyticsAnswer> askLocal(MiruRequestHandle<BM, ?> handle, Optional<TrendingReport> report) throws Exception {
        MiruSolutionLog solutionLog = new MiruSolutionLog(request.logLevel);
        MiruRequestContext<BM, ? extends MiruSipCursor<?>> context = handle.getRequestContext();

        MiruTermComposer termComposer = context.getTermComposer();

        MiruSchema schema = context.getSchema();
        int fieldId = schema.getFieldId(request.query.aggregateCountAroundField);
        MiruFieldDefinition fieldDefinition = schema.getFieldDefinition(fieldId);

        long start = System.currentTimeMillis();
        Collection<MiruTermId> termIds = Collections.emptyList();
        if (request.query.distinctQueries.size() == 1) {
            ArrayList<MiruTermId> termIdsList = Lists.newArrayList();
            distincts.gatherDirect(handle.getBitmaps(), handle.getRequestContext(), request.query.distinctQueries.get(0), solutionLog, termId -> {
                termIdsList.add(termId);
                return true;
            });
            termIds = termIdsList;
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
                termIds = joinTerms;
            }
        }
        solutionLog.log(MiruSolutionLogLevel.INFO, "Gathered {} distincts for {} queries in {} ms.",
            termIds.size(), request.query.distinctQueries.size(), (System.currentTimeMillis() - start));

        start = System.currentTimeMillis();
        Collection<MiruTermId> _termIds = termIds;
        Map<String, Waveform> waveforms = Maps.newHashMapWithExpectedSize(termIds.size());
        boolean resultsExhausted = analytics.analyze(solutionLog,
            handle,
            context,
            request.authzExpression,
            combinedTimeRange,
            request.query.constraintsFilter,
            request.query.divideTimeRangeIntoNSegments,
            (Analytics.ToAnalyze<MiruTermId> toAnalyze) -> {
                for (MiruTermId termId : _termIds) {
                    toAnalyze.analyze(termId, new MiruFilter(MiruFilterOperation.and,
                        false,
                        Collections.singletonList(MiruFieldFilter.raw(
                            MiruFieldType.primary, request.query.aggregateCountAroundField, Collections.singletonList(termId))),
                        null));
                }
                return true;
            },
            (MiruTermId term, Waveform waveform) -> {
                waveforms.put(termComposer.decompose(fieldDefinition, term), waveform);
                return true;
            });
        solutionLog.log(MiruSolutionLogLevel.INFO, "Analyzed {} waveforms in {} ms.",
            waveforms.size(), (System.currentTimeMillis() - start));

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
