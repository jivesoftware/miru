package com.jivesoftware.os.miru.analytics.plugins.analytics;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.api.query.filter.MiruValue;
import com.jivesoftware.os.miru.plugin.solution.MiruAnswerMerger;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.plugin.solution.Waveform;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class AnalyticsAnswerMerger implements MiruAnswerMerger<AnalyticsAnswer> {

    private final Map<String, Integer> scoreSets;

    public AnalyticsAnswerMerger(Map<String, Integer> scoreSets) {
        this.scoreSets = scoreSets;
    }

    /**
     * Merges the last and current results, returning the merged result.
     *
     * @param last          the last merge result
     * @param currentAnswer the next result to merge
     * @param solutionLog   the solution log
     * @return the merged result
     */
    @Override
    public AnalyticsAnswer merge(Optional<AnalyticsAnswer> last, AnalyticsAnswer currentAnswer, MiruSolutionLog solutionLog) {
        if (!last.isPresent()) {
            solutionLog.log(MiruSolutionLogLevel.INFO, "merge: no last answer, using current answer.");
            return currentAnswer;
        }

        Map<String, List<Waveform>> keyedMergedWaveforms = Maps.newHashMap();
        for (Map.Entry<String, Integer> entry : scoreSets.entrySet()) {
            String key = entry.getKey();
            int divideTimeRangeIntoNSegments = entry.getValue();

            List<Waveform> mergedWaveforms;
            AnalyticsAnswer lastAnswer = last.get();
            List<Waveform> currentWaveforms = currentAnswer.waveforms.get(key);
            List<Waveform> lastWaveforms = lastAnswer.waveforms.get(key);
            if (currentWaveforms == null) {
                if (lastWaveforms == null) {
                    solutionLog.log(MiruSolutionLogLevel.WARN, "merge: current and last waveforms are null.");
                    mergedWaveforms = null;
                } else {
                    solutionLog.log(MiruSolutionLogLevel.WARN, "merge: current waveforms are null, using last answer.");
                    mergedWaveforms = lastWaveforms;
                }
            } else {
                List<Waveform> biggerList = lastWaveforms.size() > currentWaveforms.size() ? lastWaveforms : currentWaveforms;
                Map<MiruValue, Waveform> smallerSet = lastWaveforms.size() > currentWaveforms.size()
                    ? toMap(currentWaveforms)
                    : toMap(lastWaveforms);

                mergeWaveform(smallerSet, biggerList, divideTimeRangeIntoNSegments, solutionLog);
                mergedWaveforms = biggerList;
                solutionLog.log(MiruSolutionLogLevel.INFO, "merge: merged key={}, last answer size={}, with current answer size={}.",
                    key, lastWaveforms.size(), currentWaveforms.size());
            }
            keyedMergedWaveforms.put(key, mergedWaveforms);
        }

        return new AnalyticsAnswer(keyedMergedWaveforms, currentAnswer.resultsExhausted);
    }

    private Map<MiruValue, Waveform> toMap(List<Waveform> waveforms) {
        Map<MiruValue, Waveform> map = Maps.newHashMapWithExpectedSize(waveforms.size());
        for (Waveform waveform : waveforms) {
            map.put(waveform.getId(), waveform);
        }
        return map;
    }

    private void mergeWaveform(Map<MiruValue, Waveform> mergedWaveforms,
        List<Waveform> waveforms,
        int divideTimeRangeIntoNSegments,
        MiruSolutionLog solutionLog) {

        long[] mergedWaveform = new long[divideTimeRangeIntoNSegments];
        for (Waveform waveform : waveforms) {

            Waveform had = mergedWaveforms.remove(waveform.getId());
            if (had != null) {
                Arrays.fill(mergedWaveform, 0);
                waveform.mergeWaveform(mergedWaveform);
                had.mergeWaveform(mergedWaveform);
                waveform.compress(mergedWaveform);
                if (solutionLog.isLogLevelEnabled(MiruSolutionLogLevel.DEBUG)) {
                    solutionLog.log(MiruSolutionLogLevel.DEBUG, "merge: key={} merging {} result {}",
                        waveform.getId(), had, waveform);
                }
            }
        }
        waveforms.addAll(mergedWaveforms.values());
    }

    @Override
    public AnalyticsAnswer done(Optional<AnalyticsAnswer> last, AnalyticsAnswer alternative, final MiruSolutionLog solutionLog) {
        return last.or(alternative);
    }

}
