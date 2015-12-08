package com.jivesoftware.os.miru.reco.plugins.distincts;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.jivesoftware.os.miru.api.query.filter.MiruValue;
import com.jivesoftware.os.miru.plugin.solution.MiruAnswerMerger;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import java.util.Set;

/**
 *
 */
public class DistinctsAnswerMerger implements MiruAnswerMerger<DistinctsAnswer> {

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
        Set<MiruValue> terms = Sets.newHashSet(lastAnswer.results);
        terms.addAll(currentAnswer.results);

        DistinctsAnswer mergedAnswer = new DistinctsAnswer(Lists.newArrayList(terms), terms.size(), currentAnswer.resultsExhausted);

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
