package com.jivesoftware.os.miru.reco.plugins.trending;

import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
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
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
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
    private final Cache<TrendingWaveformKey, TrendingVersionedWaveform> queryCache;
    private final Interner<MiruFilter> constraintsInterner;

    public TrendingInjectable(MiruProvider<? extends Miru> miruProvider,
        Distincts distincts,
        Analytics analytics) {
        this.provider = miruProvider;
        this.distincts = distincts;
        this.analytics = analytics;

        TrendingPluginConfig config = miruProvider.getConfig(TrendingPluginConfig.class);
        this.gatherDistinctsBatchSize = config.getGatherDistinctsBatchSize();

        if (config.getQueryCacheMaxSize() > 0) {
            this.queryCache = CacheBuilder.newBuilder().maximumSize(config.getQueryCacheMaxSize()).build();
            this.constraintsInterner = Interners.newWeakInterner();
        } else {
            this.queryCache = null;
            this.constraintsInterner = null;
        }
    }

    double zeroToOne(long _min, long _max, long _long) {
        if (_max == _min) {
            if (_long == _min) {
                return 0;
            }
            if (_long > _max) {
                return Double.MAX_VALUE;
            }
            return -Double.MAX_VALUE;
        }
        return (double) (_long - _min) / (double) (_max - _min);
    }

    public MiruResponse<TrendingAnswer> scoreTrending(MiruRequest<TrendingQuery> request) throws MiruQueryServiceException, InterruptedException {
        try {
            WaveformRegression regression = new WaveformRegression();
            WaveformRegression relativeRegression = new WaveformRegression();
            LOG.debug("askAndMerge: request={}", request);
            MiruTenantId tenantId = request.tenantId;
            Miru miru = provider.getMiru(tenantId);

            MiruTimeRange combinedTimeRange = request.query.timeRange;
            int divideTimeRangeIntoNSegments = request.query.divideTimeRangeIntoNSegments;
            int firstBucket = 0;
            int lastBucket = divideTimeRangeIntoNSegments;

            int firstRelativeBucket = -1;
            int lastRelativeBucket = -1;
            if (request.query.relativeChangeTimeRange != null) {
                long range = request.query.timeRange.largestTimestamp - request.query.timeRange.smallestTimestamp;
                combinedTimeRange = new MiruTimeRange(
                    Math.min(request.query.timeRange.smallestTimestamp, request.query.relativeChangeTimeRange.smallestTimestamp),
                    Math.max(request.query.timeRange.largestTimestamp, request.query.relativeChangeTimeRange.largestTimestamp));

                long combinedRange = combinedTimeRange.largestTimestamp - combinedTimeRange.smallestTimestamp;
                divideTimeRangeIntoNSegments = (int) (combinedRange / (range / divideTimeRangeIntoNSegments));

                firstBucket = (int) (divideTimeRangeIntoNSegments * zeroToOne(combinedTimeRange.smallestTimestamp, combinedTimeRange.largestTimestamp,
                    request.query.timeRange.smallestTimestamp));

                lastBucket = (int) (divideTimeRangeIntoNSegments * zeroToOne(combinedTimeRange.smallestTimestamp, combinedTimeRange.largestTimestamp,
                    request.query.timeRange.largestTimestamp));

                firstRelativeBucket = (int) (divideTimeRangeIntoNSegments * zeroToOne(combinedTimeRange.smallestTimestamp, combinedTimeRange.largestTimestamp,
                    request.query.relativeChangeTimeRange.smallestTimestamp));

                lastRelativeBucket = (int) (divideTimeRangeIntoNSegments * zeroToOne(combinedTimeRange.smallestTimestamp, combinedTimeRange.largestTimestamp,
                    request.query.relativeChangeTimeRange.largestTimestamp));

                divideTimeRangeIntoNSegments = Math.max(lastBucket, lastRelativeBucket);

                LOG.debug("BUCKETS: {} - {} {} - {} segs:{} newSegs:{}",
                    firstBucket, lastBucket, firstRelativeBucket, lastRelativeBucket,
                    request.query.divideTimeRangeIntoNSegments, divideTimeRangeIntoNSegments);
            }

            MiruResponse<AnalyticsAnswer> analyticsResponse = miru.askAndMerge(tenantId,
                new MiruSolvableFactory<>(provider.getStats(), "trending", new TrendingQuestion(distincts,
                    analytics,
                    gatherDistinctsBatchSize,
                    combinedTimeRange,
                    request,
                    provider.getRemotePartition(TrendingRemotePartition.class),
                    queryCache,
                    constraintsInterner)),
                new AnalyticsAnswerEvaluator(),
                new AnalyticsAnswerMerger(combinedTimeRange, request.query.divideTimeRangeIntoNSegments),
                AnalyticsAnswer.EMPTY_RESULTS,
                request.logLevel);

            List<Waveform> waveforms = (analyticsResponse.answer != null && analyticsResponse.answer.waveforms != null)
                ? analyticsResponse.answer.waveforms
                : Collections.<Waveform>emptyList();
            
            long[] waveform = new long[divideTimeRangeIntoNSegments];
            double bucket95 = 0;
            if (request.query.strategies.contains(Strategy.PEAKS)) {
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

            Map<Strategy, MinMaxPriorityQueue<Trendy>> strategyResults = Maps.newHashMapWithExpectedSize(request.query.strategies.size());
            for (Strategy strategy : request.query.strategies) {
                strategyResults.put(strategy,
                    MinMaxPriorityQueue
                    .maximumSize(request.query.desiredNumberOfDistincts)
                    .create());
            }

            for (Waveform entry : waveforms) {
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

                if (request.query.relativeChangeTimeRange != null) {
                    int buckets = Math.max(lastBucket - firstBucket, 1);
                    long[] actualWaveform = new long[buckets];
                    System.arraycopy(waveform, firstBucket, actualWaveform, 0, buckets);
                    entry.compress(actualWaveform);
                }

                if (hasCounts) {
                    if (request.query.strategies.contains(Strategy.LINEAR_REGRESSION)) {
                        if (request.query.relativeChangeTimeRange != null) {
                            regression.clear();
                            regression.add(waveform, firstBucket, lastBucket);
                            relativeRegression.clear();
                            relativeRegression.add(waveform, firstRelativeBucket, lastRelativeBucket);
                            double rankDelta = regression.slope() - relativeRegression.slope();
                            strategyResults.get(Strategy.LINEAR_REGRESSION).add(new Trendy(entry.getId(), regression.slope(), rankDelta));
                        } else {
                            regression.clear();
                            regression.add(waveform, 0, waveform.length);
                            strategyResults.get(Strategy.LINEAR_REGRESSION).add(new Trendy(entry.getId(), regression.slope(), null));
                        }
                    }
                    if (request.query.strategies.contains(Strategy.LEADER)) {
                        if (request.query.relativeChangeTimeRange != null) {
                            long sum = 0;
                            for (int i = firstBucket; i < lastBucket; i++) {
                                sum += waveform[i];
                            }
                            long relativeSum = 0;
                            for (int i = firstRelativeBucket; i < lastRelativeBucket; i++) {
                                relativeSum += waveform[i];
                            }
                            double rankDelta = sum - relativeSum;
                            strategyResults.get(Strategy.LEADER).add(new Trendy(entry.getId(), (double) sum, rankDelta));
                        } else {
                            long sum = 0;
                            for (long w : waveform) {
                                sum += w;
                            }
                            strategyResults.get(Strategy.LEADER).add(new Trendy(entry.getId(), (double) sum, null));
                        }
                    }
                    if (request.query.strategies.contains(Strategy.PEAKS)) {
                        double threshold = (highestBucket / 6) + (bucket95 / 100);
                        if (request.query.relativeChangeTimeRange != null) {
                            List<PeakDet.Peak> peaks = peakDet.peakdet(waveform, firstBucket, lastBucket, threshold);
                            List<PeakDet.Peak> peaksRelative = peakDet.peakdet(waveform, firstRelativeBucket, lastRelativeBucket, threshold);
                            double rankDelta = peaks.size() - peaksRelative.size();
                            strategyResults.get(Strategy.PEAKS).add(new Trendy(entry.getId(), (double) peaks.size(), rankDelta));
                        } else {
                            List<PeakDet.Peak> peaks = peakDet.peakdet(waveform, threshold);
                            strategyResults.get(Strategy.PEAKS).add(new Trendy(entry.getId(), (double) peaks.size(), null));
                        }
                    }
                    if (request.query.strategies.contains(Strategy.HIGHEST_PEAK)) {
                        if (request.query.relativeChangeTimeRange != null) {
                            double max = 0;
                            for (int i = firstBucket; i < lastBucket; i++) {
                                max = Math.max(max, waveform[i]);
                            }
                            double relativeMax = 0;
                            for (int i = firstRelativeBucket; i < lastRelativeBucket; i++) {
                                relativeMax = Math.max(relativeMax, waveform[i]);
                            }
                            strategyResults.get(Strategy.HIGHEST_PEAK).add(new Trendy(entry.getId(), max, max - relativeMax));
                        } else {
                            double max = 0;
                            for (int i = 0; i < waveform.length; i++) {
                                max = Math.max(max, waveform[i]);
                            }
                            strategyResults.get(Strategy.HIGHEST_PEAK).add(new Trendy(entry.getId(), max, null));
                        }
                    }
                }
            }

            Set<String> retainKeys = Sets.newHashSet();
            Map<String, List<Trendy>> strategySortedTrendies = Maps.newHashMapWithExpectedSize(strategyResults.size());
            for (Map.Entry<Strategy, MinMaxPriorityQueue<Trendy>> entry : strategyResults.entrySet()) {
                List<Trendy> sortedTrendies = Lists.newArrayList(entry.getValue());
                Collections.sort(sortedTrendies);
                strategySortedTrendies.put(entry.getKey().name(), sortedTrendies);
                for (Trendy trendy : sortedTrendies) {
                    retainKeys.add(trendy.distinctValue);
                }
            }

            Map<String, Waveform> distinctWaveforms = Maps.newHashMap();
            for (Waveform entry : waveforms) {
                if (retainKeys.contains(entry.getId())) {
                    distinctWaveforms.put(entry.getId(), entry);
                }
            }

            ImmutableList<String> solutionLog = ImmutableList.<String>builder()
                .addAll(analyticsResponse.log)
                .build();
            LOG.debug("Solution:\n{}", solutionLog);

            return new MiruResponse<>(new TrendingAnswer(distinctWaveforms, strategySortedTrendies),
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
            LOG.debug("askImmediate: partitionId={} request={}", partitionId, request);
            LOG.trace("askImmediate: report={}", requestAndReport.report);
            MiruTenantId tenantId = request.tenantId;
            Miru miru = provider.getMiru(tenantId);

            MiruTimeRange combinedTimeRange = request.query.timeRange;
            if (request.query.relativeChangeTimeRange != null) {
                combinedTimeRange = new MiruTimeRange(
                    Math.min(request.query.timeRange.smallestTimestamp, request.query.relativeChangeTimeRange.smallestTimestamp),
                    Math.max(request.query.timeRange.largestTimestamp, request.query.relativeChangeTimeRange.largestTimestamp));
            }

            return miru.askImmediate(tenantId,
                partitionId,
                new MiruSolvableFactory<>(provider.getStats(),
                    "scoreTrending",
                    new TrendingQuestion(distincts,
                        analytics,
                        gatherDistinctsBatchSize,
                        combinedTimeRange,
                        request,
                        provider.getRemotePartition(TrendingRemotePartition.class),
                        queryCache,
                        constraintsInterner)),
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
}
