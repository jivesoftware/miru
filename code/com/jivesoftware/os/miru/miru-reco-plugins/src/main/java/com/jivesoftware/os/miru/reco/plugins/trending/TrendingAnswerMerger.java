package com.jivesoftware.os.miru.reco.plugins.trending;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.base.MiruIBA;
import com.jivesoftware.os.miru.query.MiruAnswerMerger;
import com.jivesoftware.os.miru.query.MiruTimeRange;
import com.jivesoftware.os.miru.reco.trending.SimpleRegressionTrend;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class TrendingAnswerMerger implements MiruAnswerMerger<TrendingAnswer> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final MiruTimeRange timeRange;
    private final int divideTimeRangeIntoNSegments;
    private final int desiredNumberOfDistincts;

    public TrendingAnswerMerger(MiruTimeRange timeRange, int divideTimeRangeIntoNSegments, int desiredNumberOfDistincts) {
        this.timeRange = timeRange;
        this.divideTimeRangeIntoNSegments = divideTimeRangeIntoNSegments;
        this.desiredNumberOfDistincts = desiredNumberOfDistincts;
    }

    /**
     * Merges the last and current results, returning the merged result.
     *
     * @param last the last merge result
     * @param currentAnswer the next result to merge
     * @return the merged result
     */
    @Override
    public TrendingAnswer merge(Optional<TrendingAnswer> last, TrendingAnswer currentAnswer) {
        if (!last.isPresent()) {
            return currentAnswer;
        }

        TrendingAnswer lastAnswer = last.get();

        Map<MiruIBA, TrendingAnswer.Trendy> carryOverCounts = new HashMap<>();
        for (TrendingAnswer.Trendy trendy : currentAnswer.results) {
            carryOverCounts.put(new MiruIBA(trendy.distinctValue), trendy);
        }

        int size = currentAnswer.results.size() + (last.isPresent() ? last.get().results.size() : 0);

        List<TrendingAnswer.Trendy> mergedResults = Lists.newArrayListWithCapacity(size);
        final long trendInterval = timeRange.largestTimestamp - timeRange.smallestTimestamp;
        for (TrendingAnswer.Trendy trendy : lastAnswer.results) {
            TrendingAnswer.Trendy had = carryOverCounts.remove(new MiruIBA(trendy.distinctValue));
            if (had == null) {
                mergedResults.add(trendy);
            } else {
                try {
                    SimpleRegressionTrend merged = new SimpleRegressionTrend(divideTimeRangeIntoNSegments, trendInterval);
                    merged.merge(trendy.trend);
                    merged.merge(had.trend);
                    mergedResults.add(new TrendingAnswer.Trendy(trendy.distinctValue, merged, merged.getRank(timeRange.largestTimestamp)));
                } catch (Exception e) {
                    throw new RuntimeException("Failed to merge", e);
                }
            }
        }
        for (TrendingAnswer.Trendy trendy : currentAnswer.results) {
            if (carryOverCounts.containsKey(new MiruIBA(trendy.distinctValue))) {
                mergedResults.add(trendy);
            }
        }

        TrendingAnswer mergedAnswer = new TrendingAnswer(ImmutableList.copyOf(mergedResults), currentAnswer.aggregateTerms, currentAnswer.collectedDistincts,
                currentAnswer.resultsExhausted);

        logMergeResult(currentAnswer, lastAnswer, mergedAnswer);

        return mergedAnswer;
    }

    @Override
    public TrendingAnswer done(Optional<TrendingAnswer> last, TrendingAnswer alternative) {
        return last.transform(new Function<TrendingAnswer, TrendingAnswer>() {
            @Override
            public TrendingAnswer apply(TrendingAnswer result) {
                List<TrendingAnswer.Trendy> results = Lists.newArrayList(result.results);
                long t = System.currentTimeMillis();
                Collections.sort(results);
                log.debug("Sorted in " + (System.currentTimeMillis() - t) + " ms");
                results = results.subList(0, Math.min(desiredNumberOfDistincts, results.size()));
                return new TrendingAnswer(ImmutableList.copyOf(results), result.aggregateTerms, result.collectedDistincts, result.resultsExhausted);
            }
        }).or(alternative);
    }

    private void logMergeResult(TrendingAnswer currentAnswer, TrendingAnswer lastAnswer, TrendingAnswer mergedAnswer) {
        log.debug("Merged:" +
                        "\n  From: terms={} results={} collected={}" +
                        "\n  With: terms={} results={} collected={}" +
                        "\n  To:   terms={} results={} collected={}",
                lastAnswer.aggregateTerms.size(), lastAnswer.results.size(), lastAnswer.collectedDistincts,
                currentAnswer.aggregateTerms.size(), currentAnswer.results.size(), currentAnswer.collectedDistincts,
                mergedAnswer.aggregateTerms.size(), mergedAnswer.results.size(), mergedAnswer.collectedDistincts
        );
    }
}
