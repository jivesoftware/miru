package com.jivesoftware.os.miru.reco.plugins.trending;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.analytics.plugins.analytics.Analytics;
import com.jivesoftware.os.miru.analytics.plugins.analytics.AnalyticsAnswer;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
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
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 *
 */
public class TrendingQuestion implements Question<TrendingQuery, AnalyticsAnswer, TrendingReport> {

    private final Distincts distincts;
    private final Analytics analytics;
    private final int gatherDistinctsBatchSize;
    private final MiruRequest<TrendingQuery> request;
    private final MiruTimeRange combinedTimeRange;
    private final MiruRemotePartition<TrendingQuery, AnalyticsAnswer, TrendingReport> remotePartition;

    public TrendingQuestion(Distincts distincts,
        Analytics analytics,
        int gatherDistinctsBatchSize,
        MiruTimeRange combinedTimeRange,
        MiruRequest<TrendingQuery> request,
        MiruRemotePartition<TrendingQuery, AnalyticsAnswer, TrendingReport> remotePartition) {
        this.distincts = distincts;
        this.analytics = analytics;
        this.gatherDistinctsBatchSize = gatherDistinctsBatchSize;
        this.combinedTimeRange = combinedTimeRange;
        this.request = request;
        this.remotePartition = remotePartition;
    }

    @Override
    public <BM extends IBM, IBM> MiruPartitionResponse<AnalyticsAnswer> askLocal(MiruRequestHandle<BM, IBM, ?> handle,
        Optional<TrendingReport> report) throws Exception {

        MiruSolutionLog solutionLog = new MiruSolutionLog(request.logLevel);
        MiruRequestContext<BM, IBM, ? extends MiruSipCursor<?>> context = handle.getRequestContext();
        MiruBitmaps<BM, IBM> bitmaps = handle.getBitmaps();

        MiruSchema schema = context.getSchema();
        int fieldId = schema.getFieldId(request.query.aggregateCountAroundField);
        MiruFieldDefinition fieldDefinition = schema.getFieldDefinition(fieldId);
        MiruFieldIndex<BM, IBM> primaryFieldIndex = context.getFieldIndexProvider().getFieldIndex(MiruFieldType.primary);
        StackBuffer stackBuffer = new StackBuffer();

        MiruTermComposer termComposer = context.getTermComposer();

        long upperTime = combinedTimeRange.largestTimestamp;
        long lowerTime = combinedTimeRange.smallestTimestamp;
        if (upperTime == Long.MAX_VALUE || lowerTime == 0) {
            return new MiruPartitionResponse<>(new AnalyticsAnswer(Collections.emptyList(), true), solutionLog.asList());
        }

        long start = System.currentTimeMillis();
        MiruTermId[] termIds;
        if (request.query.distinctQueries.size() == 1) {
            ArrayList<MiruTermId> termIdsList = Lists.newArrayList();
            distincts.gatherDirect(bitmaps, handle.getRequestContext(), request.query.distinctQueries.get(0), gatherDistinctsBatchSize, solutionLog,
                termId -> {
                    termIdsList.add(termId);
                    return true;
                });
            termIds = termIdsList.toArray(new MiruTermId[termIdsList.size()]);
        } else if (request.query.distinctQueries.size() > 1) {
            Set<MiruTermId> joinTerms = null;
            for (DistinctsQuery distinctQuery : request.query.distinctQueries) {
                Set<MiruTermId> queryTerms = Sets.newHashSet();
                distincts.gatherDirect(bitmaps, handle.getRequestContext(), distinctQuery, gatherDistinctsBatchSize, solutionLog,
                    termId -> {
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
                termIds = joinTerms.toArray(new MiruTermId[joinTerms.size()]);
            } else {
                termIds = new MiruTermId[0];
            }
        } else {
            termIds = new MiruTermId[0];
        }
        solutionLog.log(MiruSolutionLogLevel.INFO, "Gathered {} distincts for {} queries in {} ms.",
            termIds.length, request.query.distinctQueries.size(), (System.currentTimeMillis() - start));

        start = System.currentTimeMillis();
        List<Waveform> waveforms = Lists.newArrayListWithExpectedSize(termIds.length);
        boolean resultsExhausted = analytics.analyze(solutionLog,
            handle,
            context,
            request.authzExpression,
            combinedTimeRange,
            request.query.constraintsFilter,
            request.query.divideTimeRangeIntoNSegments,
            stackBuffer,
            (Analytics.ToAnalyze<MiruTermId, BM> toAnalyze) -> {
                bitmaps.multiTx(
                    (tx, stackBuffer1) -> primaryFieldIndex.multiTxIndex(fieldId, termIds, stackBuffer1, tx),
                    (index, bitmap) -> toAnalyze.analyze(termIds[index], bitmap),
                    stackBuffer);
                return true;
            },
            (MiruTermId termId, long[] waveformBuffer) -> {
                if (waveformBuffer != null) {
                    Waveform waveform = Waveform.compressed(termComposer.decompose(fieldDefinition, termId), waveformBuffer);
                    waveforms.add(waveform);
                }
                return true;
            });
        solutionLog.log(MiruSolutionLogLevel.INFO, "Analyzed {} waveforms in {} ms.", waveforms.size(), (System.currentTimeMillis() - start));

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
