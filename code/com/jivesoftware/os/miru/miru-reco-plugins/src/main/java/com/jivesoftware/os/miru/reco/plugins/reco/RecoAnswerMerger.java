package com.jivesoftware.os.miru.reco.plugins.reco;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.plugin.solution.MiruAnswerMerger;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class RecoAnswerMerger implements MiruAnswerMerger<RecoAnswer> {

    private final int desiredNumberOfDistincts;

    public RecoAnswerMerger(int desiredNumberOfDistincts) {
        this.desiredNumberOfDistincts = desiredNumberOfDistincts;
    }

    /**
     * Merges the last and current results, returning the merged result.
     *
     * @param last          the last merge result
     * @param currentAnswer the next result to merge
     * @param solutionLog
     * @return the merged result
     */
    @Override
    public RecoAnswer merge(Optional<RecoAnswer> last, RecoAnswer currentAnswer, MiruSolutionLog solutionLog) {
        if (!last.isPresent()) {
            return currentAnswer;
        }

        RecoAnswer lastAnswer = last.get();

        Map<String, RecoAnswer.Recommendation> ongoing = new HashMap<>();
        for (RecoAnswer.Recommendation recommendation : currentAnswer.results) {
            ongoing.put(recommendation.distinctValue, recommendation);
        }

        int size = currentAnswer.results.size() + last.get().results.size();

        List<RecoAnswer.Recommendation> mergedResults = Lists.newArrayListWithCapacity(size);
        for (RecoAnswer.Recommendation recommendation : lastAnswer.results) {
            RecoAnswer.Recommendation had = ongoing.remove(recommendation.distinctValue);
            if (had == null) {
                mergedResults.add(recommendation);
            } else {
                mergedResults.add(new RecoAnswer.Recommendation(recommendation.distinctValue, had.rank + recommendation.rank));
            }
        }
        for (RecoAnswer.Recommendation recommendation : currentAnswer.results) {
            if (ongoing.containsKey(recommendation.distinctValue)) {
                mergedResults.add(recommendation);
            }
        }
        int partitionsVisited = lastAnswer.partitionsVisited + 1;
        RecoAnswer mergedAnswer = new RecoAnswer(ImmutableList.copyOf(mergedResults), partitionsVisited);

        logMergeResult(currentAnswer, lastAnswer, mergedAnswer, solutionLog);

        return mergedAnswer;
    }

    @Override
    public RecoAnswer done(Optional<RecoAnswer> last, RecoAnswer alternative, final MiruSolutionLog solutionLog) {
        return last.transform(answer -> {
            List<RecoAnswer.Recommendation> results = Lists.newArrayList(answer.results);
            long t = System.currentTimeMillis();
            Collections.sort(results);
            solutionLog.log(MiruSolutionLogLevel.INFO, "mergeReco: sorted in {} ms", (System.currentTimeMillis() - t));
            results = results.subList(0, Math.min(desiredNumberOfDistincts, results.size()));
            return new RecoAnswer(ImmutableList.copyOf(results), answer.partitionsVisited);
        }).or(alternative);
    }

    private void logMergeResult(RecoAnswer currentAnswer, RecoAnswer lastAnswer, RecoAnswer mergedAnswer, MiruSolutionLog solutionLog) {
        solutionLog.log(MiruSolutionLogLevel.INFO, "Merged:"
                + "\n  From: results={}"
                + "\n  With: results={}"
                + "\n  To:   results={}",
            lastAnswer.results.size(),
            currentAnswer.results.size(),
            mergedAnswer.results.size());
    }
}
