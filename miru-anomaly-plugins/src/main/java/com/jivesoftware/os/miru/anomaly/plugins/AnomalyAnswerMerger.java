package com.jivesoftware.os.miru.anomaly.plugins;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.plugin.solution.MiruAnswerMerger;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import java.util.Arrays;
import java.util.Map;

/**
 *
 */
public class AnomalyAnswerMerger implements MiruAnswerMerger<AnomalyAnswer> {

    public AnomalyAnswerMerger() {}

    /**
     * Merges the last and current results, returning the merged result.
     *
     * @param last          the last merge result
     * @param currentAnswer the next result to merge
     * @param solutionLog   the solution log
     * @return the merged result
     */
    @Override
    public AnomalyAnswer merge(Optional<AnomalyAnswer> last, AnomalyAnswer currentAnswer, MiruSolutionLog solutionLog) {
        if (!last.isPresent()) {
            solutionLog.log(MiruSolutionLogLevel.INFO, "merge: no last answer, using current answer.");
            return currentAnswer;
        }

        Map<String, AnomalyAnswer.Waveform> mergedWaveforms;
        AnomalyAnswer lastAnswer = last.get();
        if (currentAnswer.waveforms == null) {
            if (lastAnswer.waveforms == null) {
                solutionLog.log(MiruSolutionLogLevel.WARN, "merge: current and last waveforms are null.");
                mergedWaveforms = null;
            } else {
                solutionLog.log(MiruSolutionLogLevel.WARN, "merge: current waveforms are null, using last answer.");
                mergedWaveforms = lastAnswer.waveforms;
            }
        } else {
            mergedWaveforms = Maps.newHashMapWithExpectedSize(Math.max(currentAnswer.waveforms.size(), lastAnswer.waveforms.size()));
            mergeWaveform(mergedWaveforms, currentAnswer, solutionLog);
            mergeWaveform(mergedWaveforms, lastAnswer, solutionLog);
            solutionLog.log(MiruSolutionLogLevel.INFO, "merge: merged last answer size={}, with current answer size={}.",
                lastAnswer.waveforms.size(), currentAnswer.waveforms.size());
        }

        return new AnomalyAnswer(mergedWaveforms, currentAnswer.resultsExhausted);
    }

    private void mergeWaveform(Map<String, AnomalyAnswer.Waveform> mergedWaveforms, AnomalyAnswer addAnswer, MiruSolutionLog solutionLog) {
        for (Map.Entry<String, AnomalyAnswer.Waveform> addEntry : addAnswer.waveforms.entrySet()) {
            String key = addEntry.getKey();
            AnomalyAnswer.Waveform addWaveform = addEntry.getValue();
            AnomalyAnswer.Waveform mergedWaveform = mergedWaveforms.get(key);
            if (mergedWaveform == null) {
                mergedWaveform = new AnomalyAnswer.Waveform(new long[addWaveform.waveform.length]);
                mergedWaveforms.put(key, mergedWaveform);
            }

            for (int i = 0; i < mergedWaveform.waveform.length; i++) {
                mergedWaveform.waveform[i] += addWaveform.waveform[i];
            }

            if (solutionLog.isLogLevelEnabled(MiruSolutionLogLevel.DEBUG)) {
                solutionLog.log(MiruSolutionLogLevel.DEBUG, "merge: key={} merged {} into {}",
                    key, Arrays.toString(addWaveform.waveform), Arrays.toString(mergedWaveform.waveform));
            }
        }
    }

    @Override
    public AnomalyAnswer done(Optional<AnomalyAnswer> last, AnomalyAnswer alternative, final MiruSolutionLog solutionLog) {
        return last.or(alternative);
    }

}
