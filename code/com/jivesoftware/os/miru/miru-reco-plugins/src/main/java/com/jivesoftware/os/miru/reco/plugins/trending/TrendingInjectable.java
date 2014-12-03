package com.jivesoftware.os.miru.reco.plugins.trending;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.analytics.plugins.analytics.Analytics;
import com.jivesoftware.os.miru.analytics.plugins.analytics.AnalyticsAnswer;
import com.jivesoftware.os.miru.analytics.plugins.analytics.AnalyticsAnswerEvaluator;
import com.jivesoftware.os.miru.analytics.plugins.analytics.AnalyticsAnswerMerger;
import com.jivesoftware.os.miru.analytics.plugins.analytics.AnalyticsQuery;
import com.jivesoftware.os.miru.analytics.plugins.analytics.AnalyticsQuestion;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.query.filter.MiruFieldFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilterOperation;
import com.jivesoftware.os.miru.plugin.Miru;
import com.jivesoftware.os.miru.plugin.MiruProvider;
import com.jivesoftware.os.miru.plugin.partition.MiruPartitionUnavailableException;
import com.jivesoftware.os.miru.plugin.solution.MiruPartitionResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruRequestAndReport;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruSolution;
import com.jivesoftware.os.miru.plugin.solution.MiruSolvableFactory;
import com.jivesoftware.os.miru.reco.plugins.distincts.Distincts;
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsAnswer;
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsAnswerEvaluator;
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsAnswerMerger;
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsQuery;
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsQuestion;
import com.jivesoftware.os.miru.reco.trending.WaveformRegression;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.math.stat.regression.SimpleRegression;

/**
 *
 */
public class TrendingInjectable {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruProvider<? extends Miru> miruProvider;
    private final Trending trending;
    private final Distincts distincts;
    private final Analytics analytics;

    public TrendingInjectable(MiruProvider<? extends Miru> miruProvider, Trending trending, Distincts distincts, Analytics analytics) {
        this.miruProvider = miruProvider;
        this.trending = trending;
        this.distincts = distincts;
        this.analytics = analytics;
    }

    public MiruResponse<TrendingAnswer> scoreTrending(MiruRequest<TrendingQuery> request) throws MiruQueryServiceException {
        try {
            LOG.debug("askAndMerge: request={}", request);
            MiruTenantId tenantId = request.tenantId;
            Miru miru = miruProvider.getMiru(tenantId);
            MiruResponse<DistinctsAnswer> distinctsResponse = miru.askAndMerge(tenantId,
                new MiruSolvableFactory<>("trendingDistincts", new DistinctsQuestion(distincts, new MiruRequest<>(
                    request.tenantId,
                    request.actorId,
                    request.authzExpression,
                    new DistinctsQuery(request.query.timeRange, request.query.aggregateCountAroundField),
                    request.debug))),
                new DistinctsAnswerEvaluator(),
                new DistinctsAnswerMerger(request.query.timeRange),
                DistinctsAnswer.EMPTY_RESULTS,
                request.debug);
            List<MiruTermId> distinctTerms = distinctsResponse.answer.results;

            Map<String, MiruFilter> constraintsFilters = Maps.newHashMap();
            for (MiruTermId termId : distinctTerms) {
                constraintsFilters.put(new String(termId.getBytes(), Charsets.UTF_8),
                    new MiruFilter(MiruFilterOperation.and,
                        Optional.of(Collections.singletonList(new MiruFieldFilter(
                            MiruFieldType.primary, request.query.aggregateCountAroundField,
                            Collections.singletonList(new String(termId.getBytes(), Charsets.UTF_8))))),
                        Optional.of(Collections.singletonList(request.query.constraintsFilter))));
            }

            MiruResponse<AnalyticsAnswer> analyticsResponse = miru.askAndMerge(tenantId,
                new MiruSolvableFactory<>("trendingAnalytics", new AnalyticsQuestion(analytics, new MiruRequest<>(
                    request.tenantId,
                    request.actorId,
                    request.authzExpression,
                    new AnalyticsQuery(request.query.timeRange,
                        request.query.divideTimeRangeIntoNSegments,
                        request.query.constraintsFilter,
                        constraintsFilters),
                    request.debug))),
                new AnalyticsAnswerEvaluator(),
                new AnalyticsAnswerMerger(request.query.timeRange),
                AnalyticsAnswer.EMPTY_RESULTS,
                request.debug);

            Map<String, AnalyticsAnswer.Waveform> waveforms = analyticsResponse.answer.waveforms;
            MinMaxPriorityQueue<Trendy> trendies = MinMaxPriorityQueue
                .maximumSize(request.query.desiredNumberOfDistincts)
                .create();
            for (Map.Entry<String, AnalyticsAnswer.Waveform> entry : waveforms.entrySet()) {
                SimpleRegression regression = WaveformRegression.getRegression(entry.getValue().waveform);
                trendies.add(new Trendy(entry.getKey().getBytes(Charsets.UTF_8), regression.getSlope()));
            }

            List<Trendy> sortedTrendies = Lists.newArrayList(trendies);
            Collections.sort(sortedTrendies);

            ImmutableList<String> solutionLog = ImmutableList.<String>builder()
                .addAll(distinctsResponse.log)
                .addAll(analyticsResponse.log)
                .build();
            LOG.debug("Solution:\n{}", solutionLog);

            return new MiruResponse<>(new TrendingAnswer(sortedTrendies),
                ImmutableList.<MiruSolution>builder()
                    .addAll(distinctsResponse.solutions)
                    .addAll(analyticsResponse.solutions)
                    .build(),
                distinctsResponse.totalElapsed + analyticsResponse.totalElapsed,
                distinctsResponse.missingSchema || analyticsResponse.missingSchema,
                ImmutableList.<Integer>builder()
                    .addAll(distinctsResponse.incompletePartitionIds)
                    .addAll(analyticsResponse.incompletePartitionIds)
                    .build(),
                solutionLog);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to score trending stream", e);
        }
    }

    public MiruResponse<OldTrendingAnswer> old_scoreTrending(MiruRequest<TrendingQuery> request) throws MiruQueryServiceException {
        try {
            LOG.debug("askAndMerge: request={}", request);
            MiruTenantId tenantId = request.tenantId;
            Miru miru = miruProvider.getMiru(tenantId);
            return miru.askAndMerge(tenantId,
                new MiruSolvableFactory<>("scoreTrending", new TrendingQuestion(trending, request)),
                new TrendingAnswerEvaluator(),
                new TrendingAnswerMerger(request.query.timeRange, request.query.divideTimeRangeIntoNSegments, request.query.desiredNumberOfDistincts),
                OldTrendingAnswer.EMPTY_RESULTS, request.debug);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to score trending stream", e);
        }
    }

    public MiruPartitionResponse<OldTrendingAnswer> scoreTrending(MiruPartitionId partitionId,
        MiruRequestAndReport<TrendingQuery, TrendingReport> requestAndReport)
        throws MiruQueryServiceException {
        try {
            LOG.debug("askImmediate: partitionId={} request={}", partitionId, requestAndReport.request);
            LOG.trace("askImmediate: report={}", requestAndReport.report);
            MiruTenantId tenantId = requestAndReport.request.tenantId;
            Miru miru = miruProvider.getMiru(tenantId);
            return miru.askImmediate(tenantId,
                partitionId,
                new MiruSolvableFactory<>("scoreTrending", new TrendingQuestion(trending, requestAndReport.request)),
                Optional.fromNullable(requestAndReport.report),
                OldTrendingAnswer.EMPTY_RESULTS, false);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to score trending stream for partition: " + partitionId.getId(), e);
        }
    }

}
