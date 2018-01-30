package com.jivesoftware.os.miru.reco.plugins.trending;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Sets;
import com.jivesoftware.os.miru.analytics.plugins.analytics.Analytics;
import com.jivesoftware.os.miru.analytics.plugins.analytics.AnalyticsAnswer;
import com.jivesoftware.os.miru.analytics.plugins.analytics.AnalyticsAnswerEvaluator;
import com.jivesoftware.os.miru.analytics.plugins.analytics.AnalyticsAnswerMerger;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.filter.MiruValue;
import com.jivesoftware.os.miru.plugin.Miru;
import com.jivesoftware.os.miru.plugin.MiruProvider;
import com.jivesoftware.os.miru.plugin.partition.MiruPartitionUnavailableException;
import com.jivesoftware.os.miru.plugin.solution.MiruPartitionResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruRequestAndReport;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruSolution;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.plugin.solution.MiruSolvableFactory;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.plugin.solution.Waveform;
import com.jivesoftware.os.miru.reco.plugins.distincts.Distincts;
import com.jivesoftware.os.miru.reco.plugins.trending.TrendingQuery.Strategy;
import com.jivesoftware.os.miru.reco.trending.WaveformRegression;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.math.stat.descriptive.rank.Percentile;

import static com.google.common.base.Objects.firstNonNull;

/**
 *
 */
public class TrendingInjectable {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruProvider<? extends Miru> provider;
    private final Distincts distincts;
    private final Analytics analytics;
    private final int gatherDistinctsBatchSize;

    private final PeakDet peakDet = new PeakDet();

    public TrendingInjectable(MiruProvider<? extends Miru> miruProvider,
        Distincts distincts,
        Analytics analytics) {
        this.provider = miruProvider;
        this.distincts = distincts;
        this.analytics = analytics;

        TrendingPluginConfig config = miruProvider.getConfig(TrendingPluginConfig.class);
        this.gatherDistinctsBatchSize = config.getGatherDistinctsBatchSize();
    }

    public MiruResponse<TrendingAnswer> scoreTrending(MiruRequest<TrendingQuery> request) throws MiruQueryServiceException, InterruptedException {
        try {
            if (LOG.isTraceEnabled()) {
                LOG.trace("askAndMerge scoreTrending {}: request={}", request.tenantId, request);
            }

            WaveformRegression regression = new WaveformRegression();
            MiruTenantId tenantId = request.tenantId;
            Miru miru = provider.getMiru(tenantId);

            MiruTimeRange combinedTimeRange = getCombinedTimeRange(request);

            Map<String, Integer> keyedSegments = Maps.newHashMap();
            for (TrendingQueryScoreSet scoreSet : request.query.scoreSets) {
                keyedSegments.put(scoreSet.key, scoreSet.divideTimeRangeIntoNSegments);
            }

            MiruResponse<AnalyticsAnswer> analyticsResponse = miru.askAndMerge(tenantId,
                new MiruSolvableFactory<>(request.name, provider.getStats(), "trending", new TrendingQuestion(distincts,
                    analytics,
                    gatherDistinctsBatchSize,
                    combinedTimeRange,
                    request,
                    provider.getRemotePartition(TrendingRemotePartition.class))),
                new AnalyticsAnswerEvaluator(),
                new AnalyticsAnswerMerger(keyedSegments),
                AnalyticsAnswer.EMPTY_RESULTS,
                miru.getDefaultExecutor(),
                request.logLevel);

            Map<String, List<Waveform>> keyedWaveforms = (analyticsResponse.answer != null && analyticsResponse.answer.waveforms != null)
                ? analyticsResponse.answer.waveforms
                : Collections.emptyMap();

            Map<String, List<Waveform>> keyedDistinctWaveforms = Maps.newHashMap();
            Map<String, TrendingAnswerScoreSet> keyedScoreSets = Maps.newHashMap();

            Set<MiruValue> consumed = Sets.newHashSet();

            for (TrendingQueryScoreSet queryScoreSet : request.query.scoreSets) {
                List<Waveform> waveforms = keyedWaveforms.get(queryScoreSet.key);
                if (waveforms == null) {
                    continue;
                }

                long[] waveform = new long[queryScoreSet.divideTimeRangeIntoNSegments];
                double bucket95 = 0;
                if (queryScoreSet.strategies.contains(Strategy.PEAKS)) {
                    double[] highestBuckets = new double[waveforms.size()];
                    int i = 0;
                    for (Waveform entry : waveforms) {
                        Arrays.fill(waveform, 0);
                        entry.mergeWaveform(waveform);
                        for (long w : waveform) {
                            highestBuckets[i] = Math.max(highestBuckets[i], w);
                        }
                        i++;
                    }
                    Percentile percentile = new Percentile();
                    bucket95 = percentile.evaluate(highestBuckets, 0.95);
                }

                Map<Strategy, MinMaxPriorityQueue<Trendy>> strategyResults = Maps.newHashMapWithExpectedSize(queryScoreSet.strategies.size());
                for (Strategy strategy : queryScoreSet.strategies) {
                    strategyResults.put(strategy,
                        MinMaxPriorityQueue
                            .maximumSize(queryScoreSet.desiredNumberOfDistincts)
                            .create());
                }

                for (Waveform entry : waveforms) {
                    if (consumed.contains(entry.getId())) {
                        continue;
                    }

                    Arrays.fill(waveform, 0);
                    entry.mergeWaveform(waveform);
                    boolean hasCounts = false;
                    double highestBucket = Double.MIN_VALUE;
                    for (long w : waveform) {
                        if (w > 0) {
                            hasCounts = true;
                        }
                        highestBucket = Math.max(w, highestBucket);
                    }

                    if (hasCounts) {
                        if (queryScoreSet.strategies.contains(Strategy.LINEAR_REGRESSION)) {
                            regression.clear();
                            regression.add(waveform, 0, waveform.length);
                            strategyResults.get(Strategy.LINEAR_REGRESSION).add(new Trendy(entry.getId(), regression.slope()));
                        }
                        if (queryScoreSet.strategies.contains(Strategy.LEADER)) {
                            long sum = 0;
                            for (long w : waveform) {
                                sum += w;
                            }
                            strategyResults.get(Strategy.LEADER).add(new Trendy(entry.getId(), (double) sum));
                        }
                        if (queryScoreSet.strategies.contains(Strategy.PEAKS)) {
                            double threshold = (highestBucket / 6) + (bucket95 / 100);
                            List<PeakDet.Peak> peaks = peakDet.peakdet(waveform, threshold);
                            strategyResults.get(Strategy.PEAKS).add(new Trendy(entry.getId(), (double) peaks.size()));
                        }
                        if (queryScoreSet.strategies.contains(Strategy.HIGHEST_PEAK)) {
                            double max = 0;
                            for (int i = 0; i < waveform.length; i++) {
                                max = Math.max(max, waveform[i]);
                            }
                            strategyResults.get(Strategy.HIGHEST_PEAK).add(new Trendy(entry.getId(), max));
                        }
                    }
                }

                Set<MiruValue> retainKeys = Sets.newHashSet();
                Map<String, List<Trendy>> strategySortedTrendies = Maps.newHashMapWithExpectedSize(strategyResults.size());
                for (Map.Entry<Strategy, MinMaxPriorityQueue<Trendy>> entry : strategyResults.entrySet()) {
                    List<Trendy> sortedTrendies = Lists.newArrayList(entry.getValue());
                    Collections.sort(sortedTrendies);
                    strategySortedTrendies.put(entry.getKey().name(), sortedTrendies);
                    for (Trendy trendy : sortedTrendies) {
                        retainKeys.add(trendy.distinctValue);
                    }
                }

                List<Waveform> distinctWaveforms = Lists.newArrayListWithCapacity(retainKeys.size());
                for (Waveform entry : waveforms) {
                    if (retainKeys.contains(entry.getId())) {
                        distinctWaveforms.add(entry);
                    }
                }

                consumed.addAll(retainKeys);
                keyedDistinctWaveforms.put(queryScoreSet.key, distinctWaveforms);
                keyedScoreSets.put(queryScoreSet.key, new TrendingAnswerScoreSet(strategySortedTrendies));
            }

            ImmutableList<String> solutionLog = ImmutableList.<String>builder()
                .addAll(analyticsResponse.log)
                .build();
            if (LOG.isTraceEnabled()) {
                LOG.trace("Solution for {}:\n{}", tenantId, solutionLog);
            }

            return new MiruResponse<>(new TrendingAnswer(keyedDistinctWaveforms, keyedScoreSets),
                ImmutableList.<MiruSolution>builder()
                    .addAll(firstNonNull(analyticsResponse.solutions, Collections.<MiruSolution>emptyList()))
                    .build(),
                analyticsResponse.totalElapsed,
                analyticsResponse.missingSchema,
                ImmutableList.<Integer>builder()
                    .addAll(firstNonNull(analyticsResponse.incompletePartitionIds, Collections.<Integer>emptyList()))
                    .build(),
                solutionLog);
        } catch (MiruPartitionUnavailableException | InterruptedException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to score trending stream", e);
        }
    }

    public MiruPartitionResponse<AnalyticsAnswer> scoreTrending(MiruPartitionId partitionId,
        MiruRequestAndReport<TrendingQuery, TrendingReport> requestAndReport)
        throws MiruQueryServiceException, InterruptedException {
        try {
            MiruRequest<TrendingQuery> request = requestAndReport.request;

            if (LOG.isTraceEnabled()) {
                LOG.trace("askImmediate scoreTrending for {}: partitionId={} request={}", request.tenantId, partitionId, request);
                LOG.trace("askImmediate scoreTrending for {}: report={}", request.tenantId, requestAndReport.report);
            }

            MiruTenantId tenantId = request.tenantId;
            Miru miru = provider.getMiru(tenantId);

            MiruTimeRange combinedTimeRange = getCombinedTimeRange(request);

            return miru.askImmediate(tenantId,
                partitionId,
                new MiruSolvableFactory<>(request.name, provider.getStats(),
                    "scoreTrending",
                    new TrendingQuestion(distincts,
                        analytics,
                        gatherDistinctsBatchSize,
                        combinedTimeRange,
                        request,
                        provider.getRemotePartition(TrendingRemotePartition.class))),
                Optional.fromNullable(requestAndReport.report),
                AnalyticsAnswer.EMPTY_RESULTS,
                MiruSolutionLogLevel.NONE);
        } catch (MiruPartitionUnavailableException | InterruptedException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to score trending stream for partition: " + partitionId.getId(), e);
        }
    }

    private MiruTimeRange getCombinedTimeRange(MiruRequest<TrendingQuery> request) {
        long minTimestamp = Long.MAX_VALUE;
        long maxTimestamp = Long.MIN_VALUE;
        for (TrendingQueryScoreSet scoreSet : request.query.scoreSets) {
            minTimestamp = Math.min(minTimestamp, scoreSet.timeRange.smallestTimestamp);
            maxTimestamp = Math.max(maxTimestamp, scoreSet.timeRange.largestTimestamp);
        }
        return new MiruTimeRange(minTimestamp, maxTimestamp);
    }

}
