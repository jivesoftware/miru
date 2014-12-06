package com.jivesoftware.os.miru.analytics.plugins.analytics;

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
public class AnalyticsAnswerMerger implements MiruAnswerMerger<AnalyticsAnswer> {

    private final MiruTimeRange timeRange;

    public AnalyticsAnswerMerger(MiruTimeRange timeRange) {
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
    public AnalyticsAnswer merge(Optional<AnalyticsAnswer> last, AnalyticsAnswer currentAnswer, MiruSolutionLog solutionLog) {
        if (!last.isPresent()) {
            solutionLog.log(MiruSolutionLogLevel.INFO, "merge: no last answer, using current answer.");
            return currentAnswer;
        }

        Map<String, AnalyticsAnswer.Waveform> mergedWaveforms;
        AnalyticsAnswer lastAnswer = last.get();
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

        return new AnalyticsAnswer(mergedWaveforms, currentAnswer.resultsExhausted);
    }

    private void mergeWaveform(Map<String, AnalyticsAnswer.Waveform> mergedWaveforms, AnalyticsAnswer addAnswer, MiruSolutionLog solutionLog) {
        for (Map.Entry<String, AnalyticsAnswer.Waveform> addEntry : addAnswer.waveforms.entrySet()) {
            String key = addEntry.getKey();
            AnalyticsAnswer.Waveform addWaveform = addEntry.getValue();
            AnalyticsAnswer.Waveform mergedWaveform = mergedWaveforms.get(key);
            if (mergedWaveform == null) {
                mergedWaveform = new AnalyticsAnswer.Waveform(new long[addWaveform.waveform.length]);
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
    public AnalyticsAnswer done(Optional<AnalyticsAnswer> last, AnalyticsAnswer alternative, final MiruSolutionLog solutionLog) {
        return last.transform(new Function<AnalyticsAnswer, AnalyticsAnswer>() {
            @Override
            public AnalyticsAnswer apply(AnalyticsAnswer result) {
                return new AnalyticsAnswer(result.waveforms, result.resultsExhausted);
            }
        }).or(alternative);
    }

}
