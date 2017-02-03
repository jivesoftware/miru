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
        for (HotOrNot big : bigger) {
            HotOrNot small = smallerMap.remove(big.value);
            if (small != null) {
                List<Hotness>[] features = null;
                if (big.features != null && small.features != null) {
                    features = new List[big.features.length];
                    for (int i = 0; i < features.length; i++) {
                        int sizeA = big.features[i] != null ? big.features[i].size() : 0;
                        int sizeB = small.features[i] != null ? small.features[i].size() : 0;
                        features[i] = Lists.newArrayListWithCapacity(sizeA + sizeB);
                        if (big.features[i] != null) {
                            features[i].addAll(big.features[i]);
                        }
                        if (small.features[i] != null) {
                            features[i].addAll(small.features[i]);
                        }
                    }
                } else if (big.features != null) {
                    features = big.features;
                } else if (small.features != null) {
                    features = small.features;
                }
                merged.add(new HotOrNot(big.value,
                    (bigger == lastFeatures) ? big.gatherLatestValues : small.gatherLatestValues,
                    mergeScores(big, small),
                    features,
                    (bigger == lastFeatures) ? big.timestamp : small.timestamp,
                    (bigger == lastFeatures) ? big.unread : small.unread,
                    big.count + small.count));
            } else {
                merged.add(big);
            }
        }
        merged.addAll(smallerMap.values());
        Collections.sort(merged);
        if (merged.size() > desiredNumberOfResults) {
            return new StrutAnswer(Lists.newArrayList(merged.subList(0, desiredNumberOfResults)),
                Math.max(lastAnswer.modelTotalPartitionCount, currentAnswer.modelTotalPartitionCount),
                currentAnswer.resultsExhausted);
        } else {
            return new StrutAnswer(merged,
                Math.max(lastAnswer.modelTotalPartitionCount, currentAnswer.modelTotalPartitionCount),
                currentAnswer.resultsExhausted);
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
