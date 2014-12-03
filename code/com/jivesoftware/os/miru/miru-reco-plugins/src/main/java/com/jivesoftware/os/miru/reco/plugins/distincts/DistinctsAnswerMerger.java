package com.jivesoftware.os.miru.reco.plugins.distincts;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.plugin.solution.MiruAnswerMerger;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import java.util.LinkedHashSet;

/**
 *
 */
public class DistinctsAnswerMerger implements MiruAnswerMerger<DistinctsAnswer> {

    private final MiruTimeRange timeRange;

    public DistinctsAnswerMerger(MiruTimeRange timeRange) {
        this.timeRange = timeRange;
    }

    /**
     * Merges the last and current results, returning the merged result.
     *
     * @param last          the last merge result
     * @param currentAnswer the next result to merge
     * @return the merged result
     */
    @Override
    public DistinctsAnswer merge(Optional<DistinctsAnswer> last, DistinctsAnswer currentAnswer, MiruSolutionLog solutionLog) {
        if (!last.isPresent()) {
            return currentAnswer;
        }

        DistinctsAnswer lastAnswer = last.get();
        LinkedHashSet<MiruTermId> termIds = Sets.newLinkedHashSet(lastAnswer.results);
        termIds.addAll(currentAnswer.results);

        DistinctsAnswer mergedAnswer = new DistinctsAnswer(ImmutableList.copyOf(termIds), termIds.size(), currentAnswer.resultsExhausted);

        logMergeResult(currentAnswer, lastAnswer, mergedAnswer, solutionLog);

        return mergedAnswer;
    }

    @Override
    public DistinctsAnswer done(Optional<DistinctsAnswer> last, DistinctsAnswer alternative, final MiruSolutionLog solutionLog) {
        return last.or(alternative);
    }

    private void logMergeResult(DistinctsAnswer currentAnswer, DistinctsAnswer lastAnswer, DistinctsAnswer mergedAnswer, MiruSolutionLog solutionLog) {
        solutionLog.log(MiruSolutionLogLevel.INFO, "Merged:" +
                        "\n  From: results={} collected={}" +
                        "\n  With: results={} collected={}" +
                        "\n  To:   results={} collected={}",
                lastAnswer.results.size(), lastAnswer.collectedDistincts,
                currentAnswer.results.size(), currentAnswer.collectedDistincts,
                mergedAnswer.results.size(), mergedAnswer.collectedDistincts);
    }
}
