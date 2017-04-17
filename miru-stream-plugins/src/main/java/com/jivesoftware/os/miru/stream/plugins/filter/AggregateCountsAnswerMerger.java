package com.jivesoftware.os.miru.stream.plugins.filter;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.api.query.filter.MiruValue;
import com.jivesoftware.os.miru.plugin.solution.MiruAnswerMerger;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
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

        Map<String, AggregateCountsAnswerConstraint> mergedConstraints = Maps.newHashMapWithExpectedSize(currentAnswer.constraints.size());
        for (Map.Entry<String, AggregateCountsAnswerConstraint> entry : currentAnswer.constraints.entrySet()) {

            AggregateCountsAnswerConstraint currentConstraint = entry.getValue();
            AggregateCountsAnswerConstraint lastConstraint = lastAnswer.constraints.get(entry.getKey());
            if (lastConstraint == null) {
                mergedConstraints.put(entry.getKey(), currentConstraint);
            } else {

                Map<MiruValue, AggregateCount> carryOverCounts = new HashMap<>();
                for (AggregateCount lower : currentConstraint.results) {
                    carryOverCounts.put(lower.distinctValue, lower);
                }

                List<AggregateCount> mergedResults = Lists.newArrayListWithCapacity(lastConstraint.results.size() + currentConstraint.results.size());
                for (AggregateCount higher : lastConstraint.results) {
                    AggregateCount lower = carryOverCounts.remove(higher.distinctValue);
                    if (lower == null) {
                        mergedResults.add(higher);
                    } else {
                        boolean hasLower = lower.oldestTimestamp != -1;
                        mergedResults.add(new AggregateCount(higher.distinctValue,
                            higher.gatherLatestValues,
                            hasLower ? lower.gatherOldestValues : higher.gatherOldestValues,
                            higher.count + lower.count, // count may be supplied even if isLower=false
                            higher.latestTimestamp,
                            hasLower ? lower.oldestTimestamp : higher.oldestTimestamp,
                            lower.anyUnread || higher.anyUnread,
                            higher.latestUnread,
                            hasLower ? lower.oldestUnread : higher.oldestUnread));
                    }
                }
                for (AggregateCount lower : currentConstraint.results) {
                    if (carryOverCounts.containsKey(lower.distinctValue) && lower.latestTimestamp != -1) {
                        mergedResults.add(lower);
                    }
                }

                AggregateCountsAnswerConstraint mergedAnswerConstraint = new AggregateCountsAnswerConstraint(
                    mergedResults,
                    currentConstraint.aggregateTerms,
                    currentConstraint.uncollectedTerms,
                    currentConstraint.skippedDistincts,
                    currentConstraint.collectedDistincts);

                mergedConstraints.put(entry.getKey(), mergedAnswerConstraint);
            }

        }
        AggregateCountsAnswer mergedAnswer = new AggregateCountsAnswer(mergedConstraints, currentAnswer.resultsExhausted);

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

        if (solutionLog.isLogLevelEnabled(MiruSolutionLogLevel.INFO)) {

            for (Map.Entry<String, AggregateCountsAnswerConstraint> entry : currentAnswer.constraints.entrySet()) {

                AggregateCountsAnswerConstraint currentConstraint = entry.getValue();
                AggregateCountsAnswerConstraint lastConstraint = lastAnswer.constraints.get(entry.getKey());
                AggregateCountsAnswerConstraint mergedConstraint = mergedAnswer.constraints.get(entry.getKey());

                if (lastConstraint != null) {

                    solutionLog.log(MiruSolutionLogLevel.INFO, " Merged: {}"
                            + "\n  From: terms={} results={} collected={} skipped={}"
                            + "\n  With: terms={} results={} collected={} skipped={}"
                            + "\n  To:   terms={} results={} collected={} skipped={}",
                        entry.getKey(),
                        lastConstraint.aggregateTerms.size(), lastConstraint.results.size(), lastConstraint.collectedDistincts, lastConstraint.skippedDistincts,
                        currentConstraint.aggregateTerms.size(), currentConstraint.results.size(), currentConstraint.collectedDistincts,
                        currentConstraint.skippedDistincts,
                        mergedConstraint.aggregateTerms.size(), mergedConstraint.results.size(), mergedConstraint.collectedDistincts,
                        mergedConstraint.skippedDistincts);
                }
            }
        }
    }
}
