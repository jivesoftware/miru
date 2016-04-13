package com.jivesoftware.os.miru.stream.plugins.strut;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.api.query.filter.MiruValue;
import com.jivesoftware.os.miru.plugin.solution.MiruAnswerMerger;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.stream.plugins.strut.HotOrNot.Hotness;
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
                List<Hotness>[] features = null;
                if (hotOrNot.features != null && otherScore.features != null) {
                    features = new List[hotOrNot.features.length];
                    for (int i = 0; i < features.length; i++) {
                        int sizeA = hotOrNot.features[i] != null ? hotOrNot.features[i].size() : 0;
                        int sizeB = otherScore.features[i] != null ? otherScore.features[i].size() : 0;
                        features[i] = Lists.newArrayListWithCapacity(sizeA + sizeB);
                        if (hotOrNot.features[i] != null) {
                            features[i].addAll(hotOrNot.features[i]);
                        }
                        if (otherScore.features[i] != null) {
                            features[i].addAll(otherScore.features[i]);
                        }
                    }
                } else if (hotOrNot.features != null) {
                    features = hotOrNot.features;
                } else if (otherScore.features != null) {
                    features = otherScore.features;
                }
                merged.add(new HotOrNot(hotOrNot.value,
                    (bigger == lastFeatures) ? hotOrNot.gatherLatestValues : otherScore.gatherLatestValues,
                    mergeScores(hotOrNot, otherScore),
                    features,
                    (bigger == lastFeatures) ? hotOrNot.timestamp : otherScore.timestamp));
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

    private float mergeScores(HotOrNot left, HotOrNot right) {
        return Math.max(left.score, right.score);
    }

    @Override
    public StrutAnswer done(Optional<StrutAnswer> last, StrutAnswer alternative, MiruSolutionLog solutionLog) {
        return last.or(alternative);
    }

}
