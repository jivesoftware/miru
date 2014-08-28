package com.jivesoftware.os.miru.reco.plugins.trending;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.base.MiruIBA;
import com.jivesoftware.os.miru.query.MiruResultMerger;
import com.jivesoftware.os.miru.reco.trending.SimpleRegressionTrend;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 *
 */
public class MergeTrendingResults implements MiruResultMerger<TrendingResult> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final int desiredNumberOfDistincts;

    public MergeTrendingResults(int desiredNumberOfDistincts) {
        this.desiredNumberOfDistincts = desiredNumberOfDistincts;
    }

    /**
     * Merges the last and current results, returning the merged result.
     *
     * @param last the last merge result
     * @param currentResult the next result to merge
     * @return the merged result
     */
    @Override
    public TrendingResult merge(Optional<TrendingResult> last, TrendingResult currentResult) {
        if (!last.isPresent()) {
            return currentResult;
        }

        TrendingResult lastResult = last.get();

        Map<MiruIBA, TrendingResult.Trendy> carryOverCounts = new HashMap<>();
        for (TrendingResult.Trendy trendy : currentResult.results) {
            carryOverCounts.put(new MiruIBA(trendy.distinctValue), trendy);
        }

        int size = currentResult.results.size() + (last.isPresent() ? last.get().results.size() : 0);

        List<TrendingResult.Trendy> mergedResults = Lists.newArrayListWithCapacity(size);
        for (TrendingResult.Trendy trendy : lastResult.results) {
            TrendingResult.Trendy had = carryOverCounts.remove(new MiruIBA(trendy.distinctValue));
            if (had == null) {
                mergedResults.add(trendy);
            } else {
                try {
                    SimpleRegressionTrend merged = new SimpleRegressionTrend();
                    merged.merge(trendy.trend);
                    merged.merge(had.trend);
                    mergedResults.add(new TrendingResult.Trendy(trendy.distinctValue, merged, merged.getRank(merged.getCurrentT())));
                } catch (Exception e) {
                    throw new RuntimeException("Failed to merge", e);
                }
            }
        }
        for (TrendingResult.Trendy trendy : currentResult.results) {
            if (carryOverCounts.containsKey(new MiruIBA(trendy.distinctValue))) {
                mergedResults.add(trendy);
            }
        }

        TrendingResult mergedResult = new TrendingResult(ImmutableList.copyOf(mergedResults), currentResult.aggregateTerms, currentResult.collectedDistincts);

        logMergeResult(currentResult, lastResult, mergedResult);

        return mergedResult;
    }

    @Override
    public TrendingResult done(Optional<TrendingResult> last, TrendingResult alternative) {
        return last.transform(new Function<TrendingResult, TrendingResult>() {
            @Nullable
            @Override
            public TrendingResult apply(@Nullable TrendingResult result) {
                List<TrendingResult.Trendy> results = Lists.newArrayList(result.results);
                long t = System.currentTimeMillis();
                Collections.sort(results);
                log.info("mergeTrending: sorted in " + (System.currentTimeMillis() - t) + " ms");
                results = results.subList(0, Math.min(desiredNumberOfDistincts, results.size()));
                return new TrendingResult(ImmutableList.copyOf(results), result.aggregateTerms, result.collectedDistincts);
            }
        }).or(alternative);
    }

    private void logMergeResult(TrendingResult currentResult, TrendingResult lastResult, TrendingResult mergedResult) {
        log.debug("Merged:" +
                "\n  From: terms={} results={} collected={}" +
                "\n  With: terms={} results={} collected={}" +
                "\n  To:   terms={} results={} collected={}",
            lastResult.aggregateTerms.size(), lastResult.results.size(), lastResult.collectedDistincts,
            currentResult.aggregateTerms.size(), currentResult.results.size(), currentResult.collectedDistincts,
            mergedResult.aggregateTerms.size(), mergedResult.results.size(), mergedResult.collectedDistincts
        );
    }
}
