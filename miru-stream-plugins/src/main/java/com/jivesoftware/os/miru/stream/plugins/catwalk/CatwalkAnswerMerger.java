package com.jivesoftware.os.miru.stream.plugins.catwalk;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.catwalk.shared.FeatureScore;
import com.jivesoftware.os.miru.plugin.solution.MiruAnswerMerger;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @author jonathan
 */
public class CatwalkAnswerMerger implements MiruAnswerMerger<CatwalkAnswer> {

    private final int desiredNumberOfResults;

    public CatwalkAnswerMerger(int desiredNumberOfResults) {
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
    public CatwalkAnswer merge(Optional<CatwalkAnswer> last, CatwalkAnswer currentAnswer, MiruSolutionLog solutionLog) {
        if (!last.isPresent()) {
            return currentAnswer;
        }

        CatwalkAnswer lastAnswer = last.get();

        if (lastAnswer.results == null) {
            return currentAnswer;
        } else if (currentAnswer.results == null) {
            return lastAnswer;
        }

        Preconditions.checkArgument(currentAnswer.results.length == lastAnswer.results.length, "Misaligned feature arrays");

        @SuppressWarnings("unchecked")
        List<FeatureScore>[] mergedFeatures = new List[currentAnswer.results.length];
        for (int i = 0; i < currentAnswer.results.length; i++) {
            List<FeatureScore> lastFeatures = lastAnswer.results[i];
            List<FeatureScore> currentFeatures = currentAnswer.results[i];

            List<FeatureScore> bigger, smaller;
            if (lastFeatures.size() > currentFeatures.size()) {
                bigger = lastFeatures;
                smaller = currentFeatures;
            } else {
                bigger = currentFeatures;
                smaller = lastFeatures;
            }

            Map<Key, FeatureScore> smallerMap = Maps.newHashMap();
            for (FeatureScore featureScore : smaller) {
                smallerMap.put(new Key(featureScore.termIds), featureScore);
            }

            List<FeatureScore> merged = Lists.newArrayListWithCapacity(bigger.size() + smaller.size());
            for (FeatureScore featureScore : bigger) {
                FeatureScore otherScore = smallerMap.remove(new Key(featureScore.termIds));
                if (otherScore != null) {
                    long[] numerators = new long[featureScore.numerators.length];
                    for (int j = 0; j < featureScore.numerators.length; j++) {
                        numerators[j] = featureScore.numerators[j] + otherScore.numerators[j];
                    }
                    merged.add(new FeatureScore(featureScore.termIds,
                        numerators,
                        featureScore.denominator + otherScore.denominator,
                        featureScore.numPartitions + otherScore.numPartitions
                    ));
                } else {
                    merged.add(featureScore);
                }
            }
            merged.addAll(smallerMap.values());
            mergedFeatures[i] = merged;
        }

        MiruTimeRange mergedTimeRange = new MiruTimeRange(Math.min(currentAnswer.timeRange.smallestTimestamp, lastAnswer.timeRange.smallestTimestamp),
            Math.max(currentAnswer.timeRange.largestTimestamp, lastAnswer.timeRange.largestTimestamp));
        boolean mergedResultsClosed = currentAnswer.resultsClosed && lastAnswer.resultsClosed;
        long[] mergedModelCounts = new long[currentAnswer.modelCounts.length];
        for (int i = 0; i < mergedModelCounts.length; i++) {
            mergedModelCounts[i] = lastAnswer.modelCounts[i] + currentAnswer.modelCounts[i];
        }
        return new CatwalkAnswer(mergedFeatures,
            mergedModelCounts,
            lastAnswer.totalCount + currentAnswer.totalCount,
            mergedTimeRange,
            currentAnswer.resultsExhausted,
            mergedResultsClosed,
            false);
    }

    @Override
    public CatwalkAnswer done(Optional<CatwalkAnswer> last, CatwalkAnswer alternative, MiruSolutionLog solutionLog) {
        return last.or(alternative);
    }

    private static class Key {
        private final MiruTermId[] miruTermIds;

        public Key(MiruTermId[] miruTermIds) {
            this.miruTermIds = miruTermIds;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Key key = (Key) o;

            if (!Arrays.equals(miruTermIds, key.miruTermIds)) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            return miruTermIds != null ? Arrays.hashCode(miruTermIds) : 0;
        }
    }
}
