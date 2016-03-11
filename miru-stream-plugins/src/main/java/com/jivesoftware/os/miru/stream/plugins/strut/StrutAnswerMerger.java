package com.jivesoftware.os.miru.stream.plugins.strut;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.api.query.filter.MiruValue;
import com.jivesoftware.os.miru.plugin.solution.MiruAnswerMerger;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author jonathan
 */
public class StrutAnswerMerger implements MiruAnswerMerger<StrutAnswer> {

    private final int desiredNumberOfResults;

    public StrutAnswerMerger(int desiredNumberOfResults) {
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
    public StrutAnswer merge(Optional<StrutAnswer> last, StrutAnswer currentAnswer, MiruSolutionLog solutionLog) {
        if (!last.isPresent()) {
            return currentAnswer;
        }

        StrutAnswer lastAnswer = last.get();

        if (lastAnswer.results == null) {
            return currentAnswer;
        } else if (currentAnswer.results == null) {
            return lastAnswer;
        }

        List<HotOrNot> lastFeatures = lastAnswer.results;
        List<HotOrNot> currentFeatures = currentAnswer.results;

        List<HotOrNot> bigger, smaller;
        if (lastFeatures.size() > currentFeatures.size()) {
            bigger = lastFeatures;
            smaller = currentFeatures;
        } else {
            bigger = currentFeatures;
            smaller = lastFeatures;
        }

        Map<MiruValue, HotOrNot> smallerMap = Maps.newHashMap();
        for (HotOrNot hotOrNot : smaller) {
            smallerMap.put(hotOrNot.value, hotOrNot);
        }

        List<HotOrNot> merged = Lists.newArrayListWithCapacity(bigger.size() + smaller.size());
        for (HotOrNot hotOrNot : bigger) {
            HotOrNot otherScore = smallerMap.remove(hotOrNot.value);
            if (otherScore != null) {
                merged.add(new HotOrNot(hotOrNot.value, Math.max(hotOrNot.score, otherScore.score)));
            } else {
                merged.add(hotOrNot);
            }
        }
        merged.addAll(smallerMap.values());
        Collections.sort(merged);
        if (merged.size() > desiredNumberOfResults) {
            return new StrutAnswer(Lists.newArrayList(merged.subList(0, desiredNumberOfResults)), currentAnswer.resultsExhausted);
        } else {
            return new StrutAnswer(merged, currentAnswer.resultsExhausted);
        }
    }

    @Override
    public StrutAnswer done(Optional<StrutAnswer> last, StrutAnswer alternative, MiruSolutionLog solutionLog) {
        return last.or(alternative);
    }

}
