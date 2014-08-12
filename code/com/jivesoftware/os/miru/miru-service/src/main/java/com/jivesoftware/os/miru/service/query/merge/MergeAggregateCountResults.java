package com.jivesoftware.os.miru.service.query.merge;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.base.MiruIBA;
import com.jivesoftware.os.miru.api.query.result.AggregateCountsResult;
import com.jivesoftware.os.miru.api.query.result.AggregateCountsResult.AggregateCount;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author jonathan
 */
public class MergeAggregateCountResults implements MiruResultMerger<AggregateCountsResult> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    /**
     * Merges the last and current results, returning the merged result.
     *
     * @param last the last merge result
     * @param currentResult the next result to merge
     * @return the merged result
     */
    @Override
    public AggregateCountsResult merge(Optional<AggregateCountsResult> last, AggregateCountsResult currentResult) {
        if (!last.isPresent()) {
            return currentResult;
        }

        AggregateCountsResult lastResult = last.get();

        Map<MiruIBA, AggregateCount> carryOverCounts = new HashMap<>();
        for (AggregateCount aggregateCount : currentResult.results) {
            carryOverCounts.put(new MiruIBA(aggregateCount.distinctValue), aggregateCount);
        }

        List<AggregateCount> mergedResults = Lists.newLinkedList();
        for (AggregateCount aggregateCount : lastResult.results) {
            AggregateCount had = carryOverCounts.remove(new MiruIBA(aggregateCount.distinctValue));
            if (had == null) {
                mergedResults.add(aggregateCount);
            } else {
                mergedResults.add(new AggregateCount(aggregateCount.mostRecentActivity, aggregateCount.distinctValue, aggregateCount.count + had.count,
                    aggregateCount.unread || had.unread));
            }
        }
        for (AggregateCount aggregateCount : currentResult.results) {
            if (carryOverCounts.containsKey(new MiruIBA(aggregateCount.distinctValue)) && aggregateCount.mostRecentActivity != null) {
                mergedResults.add(aggregateCount);
            }
        }

        AggregateCountsResult mergedResult = new AggregateCountsResult(ImmutableList.copyOf(mergedResults), currentResult.aggregateTerms,
            currentResult.skippedDistincts, currentResult.collectedDistincts);

        logMergeResult(currentResult, lastResult, mergedResult);

        return mergedResult;
    }

    @Override
    public AggregateCountsResult done(Optional<AggregateCountsResult> last, AggregateCountsResult alternative) {
        return last.or(alternative);
    }

    private void logMergeResult(AggregateCountsResult currentResult, AggregateCountsResult lastResult, AggregateCountsResult mergedResult) {
        log.debug("Merged:" +
                "\n  From: terms={} results={} collected={} skipped={}" +
                "\n  With: terms={} results={} collected={} skipped={}" +
                "\n  To:   terms={} results={} collected={} skipped={}",
            lastResult.aggregateTerms.size(), lastResult.results.size(), lastResult.collectedDistincts, lastResult.skippedDistincts,
            currentResult.aggregateTerms.size(), currentResult.results.size(), currentResult.collectedDistincts, currentResult.skippedDistincts,
            mergedResult.aggregateTerms.size(), mergedResult.results.size(), mergedResult.collectedDistincts, mergedResult.skippedDistincts
        );
    }
}