package com.jivesoftware.os.miru.lumberyard.plugins;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.plugin.solution.MiruAnswerMerger;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import java.util.Arrays;
import java.util.Map;

/**
 *
 */
public class LumberyardAnswerMerger implements MiruAnswerMerger<LumberyardAnswer> {

    private final MiruTimeRange timeRange;

    public LumberyardAnswerMerger(MiruTimeRange timeRange) {
        this.timeRange = timeRange;
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
    public LumberyardAnswer merge(Optional<LumberyardAnswer> last, LumberyardAnswer currentAnswer, MiruSolutionLog solutionLog) {
        if (!last.isPresent()) {
            solutionLog.log(MiruSolutionLogLevel.INFO, "merge: no last answer, using current answer.");
            return currentAnswer;
        }

        Map<String, LumberyardAnswer.Waveform> mergedWaveforms;
        LumberyardAnswer lastAnswer = last.get();
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
            mergeWaveform(mergedWaveforms, lastAnswer, solutionLog);
            mergeWaveform(mergedWaveforms, currentAnswer, solutionLog);
            solutionLog.log(MiruSolutionLogLevel.INFO, "merge: merged last answer size={}, with current answer size={}.",
                lastAnswer.waveforms.size(), currentAnswer.waveforms.size());
        }

        return new LumberyardAnswer(mergedWaveforms, currentAnswer.resultsExhausted);
    }

    private void mergeWaveform(Map<String, LumberyardAnswer.Waveform> mergedWaveforms, LumberyardAnswer addAnswer, MiruSolutionLog solutionLog) {
        for (Map.Entry<String, LumberyardAnswer.Waveform> addEntry : addAnswer.waveforms.entrySet()) {
            String key = addEntry.getKey();
            LumberyardAnswer.Waveform addWaveform = addEntry.getValue();
            LumberyardAnswer.Waveform mergedWaveform = mergedWaveforms.get(key);
            if (mergedWaveform == null) {
                mergedWaveform = new LumberyardAnswer.Waveform(new long[addWaveform.waveform.length]);
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
    public LumberyardAnswer done(Optional<LumberyardAnswer> last, LumberyardAnswer alternative, final MiruSolutionLog solutionLog) {
        return last.transform(new Function<LumberyardAnswer, LumberyardAnswer>() {
            @Override
            public LumberyardAnswer apply(LumberyardAnswer result) {
                return new LumberyardAnswer(result.waveforms, result.resultsExhausted);
            }
        }).or(alternative);
    }

}
