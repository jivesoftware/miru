package com.jivesoftware.os.miru.reco.plugins.trending;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.MinMaxPriorityQueue;
import com.jivesoftware.os.miru.analytics.plugins.analytics.Analytics;
import com.jivesoftware.os.miru.analytics.plugins.analytics.AnalyticsAnswer;
import com.jivesoftware.os.miru.analytics.plugins.analytics.AnalyticsAnswerEvaluator;
import com.jivesoftware.os.miru.analytics.plugins.analytics.AnalyticsAnswerMerger;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
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
import com.jivesoftware.os.miru.reco.plugins.distincts.Distincts;
import com.jivesoftware.os.miru.reco.trending.WaveformRegression;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.math.stat.regression.SimpleRegression;

import static com.google.common.base.Objects.firstNonNull;

/**
 *
 */
public class TrendingInjectable {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruProvider<? extends Miru> provider;
    private final Distincts distincts;
    private final Analytics analytics;

    public TrendingInjectable(MiruProvider<? extends Miru> miruProvider,
        Distincts distincts,
        Analytics analytics) {
        this.provider = miruProvider;
        this.distincts = distincts;
        this.analytics = analytics;
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

    public MiruResponse<TrendingAnswer> scoreTrending(MiruRequest<TrendingQuery> request) throws MiruQueryServiceException {
        try {
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
                    combinedTimeRange,
                    request,
                    provider.getRemotePartition(TrendingRemotePartition.class))),
                new AnalyticsAnswerEvaluator(),
                new AnalyticsAnswerMerger(combinedTimeRange),
                AnalyticsAnswer.EMPTY_RESULTS,
                request.logLevel);

            Map<String, AnalyticsAnswer.Waveform> waveforms = (analyticsResponse.answer != null && analyticsResponse.answer.waveforms != null)
                ? analyticsResponse.answer.waveforms
                : Collections.<String, AnalyticsAnswer.Waveform>emptyMap();
            MinMaxPriorityQueue<Trendy> trendies = MinMaxPriorityQueue
                .maximumSize(request.query.desiredNumberOfDistincts)
                .create();

            for (Map.Entry<String, AnalyticsAnswer.Waveform> entry : waveforms.entrySet()) {
                long[] waveform = entry.getValue().waveform;
                boolean hasCounts = false;
                for (long w : waveform) {
                    if (w > 0) {
                        hasCounts = true;
                        break;
                    }
                }
                if (hasCounts) {
                    if (request.query.strategy == TrendingQuery.Strategy.LINEAR_REGRESSION) {
                        if (request.query.relativeChangeTimeRange != null) {
                            SimpleRegression regression = WaveformRegression.getRegression(waveform, firstBucket, lastBucket);
                            SimpleRegression regressionRelative = WaveformRegression.getRegression(waveform, firstRelativeBucket, lastRelativeBucket);
                            double rankDelta = regression.getSlope() - regressionRelative.getSlope();
                            int l = lastBucket - firstBucket;
                            if (l < 1) {
                                l = 1;
                            }
                            long[] copy = new long[l];
                            System.arraycopy(waveform, firstBucket, copy, 0, l);
                            trendies.add(new Trendy(entry.getKey(), regression.getSlope(), rankDelta, copy));
                        } else {
                            SimpleRegression regression = WaveformRegression.getRegression(waveform, 0, waveform.length);
                            trendies.add(new Trendy(entry.getKey(), regression.getSlope(), null, waveform));
                        }
                    } else if (request.query.strategy == TrendingQuery.Strategy.LEADER) {
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
                            int l = lastBucket - firstBucket;
                            if (l < 1) {
                                l = 1;
                            }
                            long[] copy = new long[l];
                            System.arraycopy(waveform, firstBucket, copy, 0, l);
                            trendies.add(new Trendy(entry.getKey(), (double) sum, rankDelta, copy));
                        } else {
                            long sum = 0;
                            for (long w : waveform) {
                                sum += w;
                            }
                            trendies.add(new Trendy(entry.getKey(), (double) sum, null, waveform));
                        }
                    } else if (request.query.strategy == TrendingQuery.Strategy.PEAKS) {
                        if (request.query.relativeChangeTimeRange != null) {
                            long sum = 0;
                            for (int i = firstBucket + 1; i < lastBucket; i++) {
                                sum += (waveform[i] - waveform[i - 1]);
                            }
                            long relativeSum = 0;
                            for (int i = firstRelativeBucket + 1; i < lastRelativeBucket; i++) {
                                relativeSum += (waveform[i] - waveform[i - 1]);
                            }
                            double rankDelta = sum - relativeSum;
                            int l = lastBucket - firstBucket;
                            if (l < 1) {
                                l = 1;
                            }
                            long[] copy = new long[l];
                            System.arraycopy(waveform, firstBucket, copy, 0, l);
                            trendies.add(new Trendy(entry.getKey(), (double) sum, rankDelta, copy));
                        } else {
                            long sum = 0;
                            for (long w : waveform) {
                                sum += w;
                            }
                            trendies.add(new Trendy(entry.getKey(), (double) sum, null, waveform));
                        }
                    }
                }
            }

            List<Trendy> sortedTrendies = Lists.newArrayList(trendies);
            Collections.sort(sortedTrendies); // Ahhh what is the point of this should already be in sort order?

            ImmutableList<String> solutionLog = ImmutableList.<String>builder()
                .addAll(analyticsResponse.log)
                .build();
            LOG.debug("Solution:\n{}", solutionLog);

            return new MiruResponse<>(new TrendingAnswer(sortedTrendies),
                ImmutableList.<MiruSolution>builder()
                    .addAll(firstNonNull(analyticsResponse.solutions, Collections.<MiruSolution>emptyList()))
                    .build(),
                analyticsResponse.totalElapsed,
                analyticsResponse.missingSchema,
                ImmutableList.<Integer>builder()
                    .addAll(firstNonNull(analyticsResponse.incompletePartitionIds, Collections.<Integer>emptyList()))
                    .build(),
                solutionLog);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to score trending stream", e);
        }
    }

    public MiruPartitionResponse<AnalyticsAnswer> scoreTrending(MiruPartitionId partitionId,
        MiruRequestAndReport<TrendingQuery, TrendingReport> requestAndReport)
        throws MiruQueryServiceException {
        try {
            LOG.debug("askImmediate: partitionId={} request={}", partitionId, requestAndReport.request);
            LOG.trace("askImmediate: report={}", requestAndReport.report);
            MiruTenantId tenantId = requestAndReport.request.tenantId;
            Miru miru = provider.getMiru(tenantId);
            return miru.askImmediate(tenantId,
                partitionId,
                new MiruSolvableFactory<>(provider.getStats(),
                    "scoreTrending",
                    new TrendingQuestion(distincts,
                        analytics,
                        requestAndReport.report.combinedTimeRange,
                        requestAndReport.request,
                        provider.getRemotePartition(TrendingRemotePartition.class))),
                Optional.fromNullable(requestAndReport.report),
                AnalyticsAnswer.EMPTY_RESULTS,
                MiruSolutionLogLevel.NONE);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to score trending stream for partition: " + partitionId.getId(), e);
        }
    }
}
