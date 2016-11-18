package com.jivesoftware.os.miru.stream.plugins.fulltext;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.plugin.solution.MiruAnswerMerger;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.stream.plugins.fulltext.FullTextAnswer.ActivityScore;
import java.util.Iterator;
import java.util.List;

/**
 * @author jonathan
 */
public class FullTextAnswerMerger implements MiruAnswerMerger<FullTextAnswer> {

    private final int desiredNumberOfResults;

    public FullTextAnswerMerger(int desiredNumberOfResults) {
        this.desiredNumberOfResults = desiredNumberOfResults;
    }

    /**
     * Merges the last and current results, returning the merged answer.
     *
     * @param last          the last merge result
     * @param currentAnswer the next result to merge
     * @return the merged answer
     */
    @Override
    public FullTextAnswer merge(Optional<FullTextAnswer> last, FullTextAnswer currentAnswer, MiruSolutionLog solutionLog) {
        if (!last.isPresent()) {
            return currentAnswer;
        }

        FullTextAnswer lastAnswer = last.get();

        List<ActivityScore> mergedResults = Lists.newArrayListWithCapacity(Math.min(
            lastAnswer.results.size() + currentAnswer.results.size(),
            desiredNumberOfResults));

        Iterator<ActivityScore> lastIter = lastAnswer.results.iterator();
        Iterator<ActivityScore> currentIter = currentAnswer.results.iterator();

        ActivityScore lastCursor = lastIter.hasNext() ? lastIter.next() : null;
        ActivityScore currentCursor = currentIter.hasNext() ? currentIter.next() : null;

        while ((lastCursor != null || currentCursor != null) && (mergedResults.size() < desiredNumberOfResults)) {
            if (lastCursor == null) {
                mergedResults.add(currentCursor);
                currentCursor = currentIter.hasNext() ? currentIter.next() : null;
            } else if (currentCursor == null) {
                mergedResults.add(lastCursor);
                lastCursor = lastIter.hasNext() ? lastIter.next() : null;
            } else {
                int c = lastCursor.compareTo(currentCursor);
                if (c < 0) {
                    mergedResults.add(lastCursor);
                    lastCursor = lastIter.hasNext() ? lastIter.next() : null;
                } else {
                    mergedResults.add(currentCursor);
                    currentCursor = currentIter.hasNext() ? currentIter.next() : null;
                }
            }
        }

        return new FullTextAnswer(ImmutableList.copyOf(mergedResults), lastAnswer.found + currentAnswer.found, currentAnswer.resultsExhausted);
    }

    @Override
    public FullTextAnswer done(Optional<FullTextAnswer> last, FullTextAnswer alternative, MiruSolutionLog solutionLog) {
        return last.or(alternative);
    }
}
