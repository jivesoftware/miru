package com.jivesoftware.os.miru.reco.plugins.trending;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.api.base.MiruIBA;
import com.jivesoftware.os.miru.plugin.solution.MiruAnswerMerger;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.reco.trending.SimpleRegressionTrend;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class TrendingAnswerMerger implements MiruAnswerMerger<OldTrendingAnswer> {

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
     * @param last          the last merge result
     * @param currentAnswer the next result to merge
     * @return the merged result
     */
    @Override
    public OldTrendingAnswer merge(Optional<OldTrendingAnswer> last, OldTrendingAnswer currentAnswer, MiruSolutionLog solutionLog) {
        if (!last.isPresent()) {
            return currentAnswer;
        }

        OldTrendingAnswer lastAnswer = last.get();

        Map<MiruIBA, OldTrendingAnswer.Trendy> carryOverCounts = new HashMap<>();
        for (OldTrendingAnswer.Trendy trendy : currentAnswer.results) {
            carryOverCounts.put(new MiruIBA(trendy.distinctValue), trendy);
        }

        int size = currentAnswer.results.size() + (last.isPresent() ? last.get().results.size() : 0);

        List<OldTrendingAnswer.Trendy> mergedResults = Lists.newArrayListWithCapacity(size);
        final long trendInterval = timeRange.largestTimestamp - timeRange.smallestTimestamp;
        for (OldTrendingAnswer.Trendy trendy : lastAnswer.results) {
            OldTrendingAnswer.Trendy had = carryOverCounts.remove(new MiruIBA(trendy.distinctValue));
            if (had == null) {
                mergedResults.add(trendy);
            } else {
                try {
                    SimpleRegressionTrend merged = new SimpleRegressionTrend(divideTimeRangeIntoNSegments, trendInterval);
                    merged.merge(trendy.trend);
                    merged.merge(had.trend);
                    mergedResults.add(new OldTrendingAnswer.Trendy(trendy.distinctValue, merged, merged.getRank(timeRange.largestTimestamp)));
                } catch (Exception e) {
                    throw new RuntimeException("Failed to merge", e);
                }
            }
        }
        for (OldTrendingAnswer.Trendy trendy : currentAnswer.results) {
            if (carryOverCounts.containsKey(new MiruIBA(trendy.distinctValue))) {
                mergedResults.add(trendy);
            }
        }

        OldTrendingAnswer mergedAnswer = new OldTrendingAnswer(ImmutableList.copyOf(mergedResults), currentAnswer.aggregateTerms,
            currentAnswer.collectedDistincts, currentAnswer.resultsExhausted);

        logMergeResult(currentAnswer, lastAnswer, mergedAnswer, solutionLog);

        return mergedAnswer;
    }

    @Override
    public OldTrendingAnswer done(Optional<OldTrendingAnswer> last, OldTrendingAnswer alternative, final MiruSolutionLog solutionLog) {
        return last.transform(new Function<OldTrendingAnswer, OldTrendingAnswer>() {
            @Override
            public OldTrendingAnswer apply(OldTrendingAnswer result) {
                List<OldTrendingAnswer.Trendy> results = Lists.newArrayList(result.results);
                long t = System.currentTimeMillis();
                Collections.sort(results);
                solutionLog.log("mergeTrendy: Sorted in {} ms", (System.currentTimeMillis() - t));
                results = results.subList(0, Math.min(desiredNumberOfDistincts, results.size()));
                return new OldTrendingAnswer(ImmutableList.copyOf(results), result.aggregateTerms, result.collectedDistincts, result.resultsExhausted);
            }
        }).or(alternative);
    }

    private void logMergeResult(OldTrendingAnswer currentAnswer, OldTrendingAnswer lastAnswer, OldTrendingAnswer mergedAnswer, MiruSolutionLog solutionLog) {
        solutionLog.log("Merged:" +
                "\n  From: terms={} results={} collected={}" +
                "\n  With: terms={} results={} collected={}" +
                "\n  To:   terms={} results={} collected={}",
            lastAnswer.aggregateTerms.size(), lastAnswer.results.size(), lastAnswer.collectedDistincts,
            currentAnswer.aggregateTerms.size(), currentAnswer.results.size(), currentAnswer.collectedDistincts,
            mergedAnswer.aggregateTerms.size(), mergedAnswer.results.size(), mergedAnswer.collectedDistincts);
    }
}
