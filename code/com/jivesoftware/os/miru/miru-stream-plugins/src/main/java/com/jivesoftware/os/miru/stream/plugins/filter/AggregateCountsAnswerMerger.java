package com.jivesoftware.os.miru.stream.plugins.filter;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.base.MiruIBA;
import com.jivesoftware.os.miru.query.MiruAnswerMerger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.jivesoftware.os.miru.stream.plugins.filter.AggregateCountsAnswer.AggregateCount;

/**
 * @author jonathan
 */
public class AggregateCountsAnswerMerger implements MiruAnswerMerger<AggregateCountsAnswer> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    /**
     * Merges the last and current results, returning the merged result.
     *
     * @param last the last merge result
     * @param currentAnswer the next result to merge
     * @return the merged result
     */
    @Override
    public AggregateCountsAnswer merge(Optional<AggregateCountsAnswer> last, AggregateCountsAnswer currentAnswer) {
        if (!last.isPresent()) {
            return currentAnswer;
        }

        AggregateCountsAnswer lastAnswer = last.get();

        Map<MiruIBA, AggregateCount> carryOverCounts = new HashMap<>();
        for (AggregateCount aggregateCount : currentAnswer.results) {
            carryOverCounts.put(new MiruIBA(aggregateCount.distinctValue), aggregateCount);
        }

        List<AggregateCount> mergedResults = Lists.newLinkedList();
        for (AggregateCount aggregateCount : lastAnswer.results) {
            AggregateCount had = carryOverCounts.remove(new MiruIBA(aggregateCount.distinctValue));
            if (had == null) {
                mergedResults.add(aggregateCount);
            } else {
                mergedResults.add(new AggregateCount(aggregateCount.mostRecentActivity, aggregateCount.distinctValue, aggregateCount.count + had.count,
                    aggregateCount.unread || had.unread));
            }
        }
        for (AggregateCount aggregateCount : currentAnswer.results) {
            if (carryOverCounts.containsKey(new MiruIBA(aggregateCount.distinctValue)) && aggregateCount.mostRecentActivity != null) {
                mergedResults.add(aggregateCount);
            }
        }

        AggregateCountsAnswer mergedAnswer = new AggregateCountsAnswer(ImmutableList.copyOf(mergedResults), currentAnswer.aggregateTerms,
            currentAnswer.skippedDistincts, currentAnswer.collectedDistincts);

        logMergeResult(currentAnswer, lastAnswer, mergedAnswer);

        return mergedAnswer;
    }

    @Override
    public AggregateCountsAnswer done(Optional<AggregateCountsAnswer> last, AggregateCountsAnswer alternative) {
        return last.or(alternative);
    }

    private void logMergeResult(AggregateCountsAnswer currentAnswer, AggregateCountsAnswer lastAnswer, AggregateCountsAnswer mergedAnswer) {
        log.debug("Merged:" +
                        "\n  From: terms={} results={} collected={} skipped={}" +
                        "\n  With: terms={} results={} collected={} skipped={}" +
                        "\n  To:   terms={} results={} collected={} skipped={}",
                lastAnswer.aggregateTerms.size(), lastAnswer.results.size(), lastAnswer.collectedDistincts, lastAnswer.skippedDistincts,
                currentAnswer.aggregateTerms.size(), currentAnswer.results.size(), currentAnswer.collectedDistincts, currentAnswer.skippedDistincts,
                mergedAnswer.aggregateTerms.size(), mergedAnswer.results.size(), mergedAnswer.collectedDistincts, mergedAnswer.skippedDistincts
        );
    }
}