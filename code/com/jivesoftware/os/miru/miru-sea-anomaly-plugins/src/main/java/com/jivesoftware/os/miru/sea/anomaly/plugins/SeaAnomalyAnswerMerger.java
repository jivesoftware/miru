package com.jivesoftware.os.miru.sea.anomaly.plugins;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.plugin.solution.MiruAnswerMerger;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import java.util.Arrays;
import java.util.Map;

/**
 *
 */
public class SeaAnomalyAnswerMerger implements MiruAnswerMerger<SeaAnomalyAnswer> {

    private final int desiredNumberOfResultsPerWaveform;

    public SeaAnomalyAnswerMerger(int desiredNumberOfResultsPerWaveform) {
        this.desiredNumberOfResultsPerWaveform = desiredNumberOfResultsPerWaveform;
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
    public SeaAnomalyAnswer merge(Optional<SeaAnomalyAnswer> last, SeaAnomalyAnswer currentAnswer, MiruSolutionLog solutionLog) {
        if (!last.isPresent()) {
            solutionLog.log(MiruSolutionLogLevel.INFO, "merge: no last answer, using current answer.");
            return currentAnswer;
        }

        Map<String, SeaAnomalyAnswer.Waveform> mergedWaveforms;
        SeaAnomalyAnswer lastAnswer = last.get();
        if (currentAnswer.waveforms == null) {
            if (lastAnswer.waveforms == null) {
                solutionLog.log(MiruSolutionLogLevel.WARN, "merge: current and last waveforms are null.");
                mergedWaveforms = null;
            } else {
                solutionLog.log(MiruSolutionLogLevel.WARN, "merge: current waveforms are null, using last answer.");
                mergedWaveforms = lastAnswer.waveforms;
            }
        } else {
            mergedWaveforms = Maps.newHashMap();
            mergeWaveform(mergedWaveforms, currentAnswer, solutionLog);
            mergeWaveform(mergedWaveforms, lastAnswer, solutionLog);
            solutionLog.log(MiruSolutionLogLevel.INFO, "merge: merged last answer size={}, with current answer size={}.",
                lastAnswer.waveforms.size(), currentAnswer.waveforms.size());
        }

        return new SeaAnomalyAnswer(mergedWaveforms, currentAnswer.resultsExhausted);
    }

    private void mergeWaveform(Map<String, SeaAnomalyAnswer.Waveform> mergedWaveforms, SeaAnomalyAnswer addAnswer, MiruSolutionLog solutionLog) {
        for (Map.Entry<String, SeaAnomalyAnswer.Waveform> addEntry : addAnswer.waveforms.entrySet()) {
            String key = addEntry.getKey();
            SeaAnomalyAnswer.Waveform addWaveform = addEntry.getValue();
            SeaAnomalyAnswer.Waveform mergedWaveform = mergedWaveforms.get(key);
            if (mergedWaveform == null) {
                mergedWaveform = new SeaAnomalyAnswer.Waveform(new long[addWaveform.waveform.length],
                    Lists.<MiruActivity>newArrayListWithCapacity(addWaveform.results.size()));
                mergedWaveforms.put(key, mergedWaveform);
            }

            for (int i = 0; i < mergedWaveform.waveform.length; i++) {
                mergedWaveform.waveform[i] += addWaveform.waveform[i];
            }

            int remainingCount = desiredNumberOfResultsPerWaveform - mergedWaveform.results.size();
            if (remainingCount > 0) {
                mergedWaveform.results.addAll(addWaveform.results.subList(0, Math.min(addWaveform.results.size(), remainingCount)));
            }

            if (solutionLog.isLogLevelEnabled(MiruSolutionLogLevel.DEBUG)) {
                solutionLog.log(MiruSolutionLogLevel.DEBUG, "merge: key={} merged {} into {}",
                    key, Arrays.toString(addWaveform.waveform), Arrays.toString(mergedWaveform.waveform));
            }
        }
    }

    @Override
    public SeaAnomalyAnswer done(Optional<SeaAnomalyAnswer> last, SeaAnomalyAnswer alternative, final MiruSolutionLog solutionLog) {
        return last.or(alternative);
    }

}
