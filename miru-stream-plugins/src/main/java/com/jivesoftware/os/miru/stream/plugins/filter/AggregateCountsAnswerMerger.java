package com.jivesoftware.os.miru.stream.plugins.filter;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.plugin.solution.MiruAnswerMerger;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.stream.plugins.filter.AggregateCountsAnswer.AggregateCount;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author jonathan
 */
public class AggregateCountsAnswerMerger implements MiruAnswerMerger<AggregateCountsAnswer> {

    /**
     * Merges the last and current results, returning the merged result.
     *
     * @param last          the last merge result
     * @param currentAnswer the next result to merge
     * @return the merged result
     */
    @Override
    public AggregateCountsAnswer merge(Optional<AggregateCountsAnswer> last, AggregateCountsAnswer currentAnswer, MiruSolutionLog solutionLog) {
        if (!last.isPresent()) {
            return currentAnswer;
        }

        AggregateCountsAnswer lastAnswer = last.get();

        Map<String, AggregateCount> carryOverCounts = new HashMap<>();
        for (AggregateCount aggregateCount : currentAnswer.results) {
            carryOverCounts.put(aggregateCount.distinctValue, aggregateCount);
        }

        List<AggregateCount> mergedResults = Lists.newArrayListWithCapacity(lastAnswer.results.size() + currentAnswer.results.size());
        for (AggregateCount aggregateCount : lastAnswer.results) {
            AggregateCount had = carryOverCounts.remove(aggregateCount.distinctValue);
            if (had == null) {
                mergedResults.add(aggregateCount);
            } else {
                mergedResults.add(new AggregateCount(aggregateCount.mostRecentActivity, aggregateCount.distinctValue, aggregateCount.count + had.count,
                    aggregateCount.unread || had.unread));
            }
        }
        for (AggregateCount aggregateCount : currentAnswer.results) {
            if (carryOverCounts.containsKey(aggregateCount.distinctValue) && aggregateCount.mostRecentActivity != null) {
                mergedResults.add(aggregateCount);
            }
        }

        AggregateCountsAnswer mergedAnswer = new AggregateCountsAnswer(ImmutableList.copyOf(mergedResults), currentAnswer.aggregateTerms,
            currentAnswer.skippedDistincts, currentAnswer.collectedDistincts, currentAnswer.resultsExhausted);

        logMergeResult(currentAnswer, lastAnswer, mergedAnswer, solutionLog);

        return mergedAnswer;
    }

    @Override
    public AggregateCountsAnswer done(Optional<AggregateCountsAnswer> last, AggregateCountsAnswer alternative, MiruSolutionLog solutionLog) {
        return last.or(alternative);
    }

    private void logMergeResult(AggregateCountsAnswer currentAnswer,
        AggregateCountsAnswer lastAnswer,
        AggregateCountsAnswer mergedAnswer,
        MiruSolutionLog solutionLog) {

        solutionLog.log(MiruSolutionLogLevel.INFO, "Merged:" +
                "\n  From: terms={} results={} collected={} skipped={}" +
                "\n  With: terms={} results={} collected={} skipped={}" +
                "\n  To:   terms={} results={} collected={} skipped={}",
            lastAnswer.aggregateTerms.size(), lastAnswer.results.size(), lastAnswer.collectedDistincts, lastAnswer.skippedDistincts,
            currentAnswer.aggregateTerms.size(), currentAnswer.results.size(), currentAnswer.collectedDistincts, currentAnswer.skippedDistincts,
            mergedAnswer.aggregateTerms.size(), mergedAnswer.results.size(), mergedAnswer.collectedDistincts, mergedAnswer.skippedDistincts);
    }
}
