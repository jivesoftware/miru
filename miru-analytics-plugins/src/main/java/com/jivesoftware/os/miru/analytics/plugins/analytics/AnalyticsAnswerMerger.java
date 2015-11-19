package com.jivesoftware.os.miru.analytics.plugins.analytics;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.plugin.solution.MiruAnswerMerger;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.plugin.solution.Waveform;
import java.util.Arrays;
import java.util.Map;

/**
 *
 */
public class AnalyticsAnswerMerger implements MiruAnswerMerger<AnalyticsAnswer> {

    private final MiruTimeRange timeRange;
    private final int divideTimeRangeIntoNSegments;

    public AnalyticsAnswerMerger(MiruTimeRange timeRange, int divideTimeRangeIntoNSegments) {
        this.timeRange = timeRange;
        this.divideTimeRangeIntoNSegments = divideTimeRangeIntoNSegments;
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

        Map<String, Waveform> mergedWaveforms;
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
            mergedWaveforms = Maps.newHashMapWithExpectedSize(Math.max(lastAnswer.waveforms.size(), currentAnswer.waveforms.size()));
            mergeWaveform(mergedWaveforms, lastAnswer, solutionLog);
            mergeWaveform(mergedWaveforms, currentAnswer, solutionLog);
            solutionLog.log(MiruSolutionLogLevel.INFO, "merge: merged last answer size={}, with current answer size={}.",
                lastAnswer.waveforms.size(), currentAnswer.waveforms.size());
        }

        return new AnalyticsAnswer(mergedWaveforms, currentAnswer.resultsExhausted);
    }

    private void mergeWaveform(Map<String, Waveform> mergedWaveforms, AnalyticsAnswer addAnswer, MiruSolutionLog solutionLog) {
        long[] waveform = new long[divideTimeRangeIntoNSegments];
        for (Map.Entry<String, Waveform> addEntry : addAnswer.waveforms.entrySet()) {
            String key = addEntry.getKey();
            Waveform addWaveform = addEntry.getValue();
            mergedWaveforms.compute(key, (s, existing) -> {
                if (existing != null) {
                    Arrays.fill(waveform, 0);
                    addWaveform.mergeWaveform(waveform);
                    existing.mergeWaveform(waveform);
                    existing.compress(waveform);
                    if (solutionLog.isLogLevelEnabled(MiruSolutionLogLevel.DEBUG)) {
                        solutionLog.log(MiruSolutionLogLevel.DEBUG, "merge: key={} merging {} result {}",
                            key, addWaveform, existing);
                    }
                    return existing;
                } else {
                    if (solutionLog.isLogLevelEnabled(MiruSolutionLogLevel.DEBUG)) {
                        solutionLog.log(MiruSolutionLogLevel.DEBUG, "merge: key={} merged {} into {}",
                            key, addWaveform, addWaveform);
                    }
                    return addWaveform;
                }
            });
        }
    }

    @Override
    public AnalyticsAnswer done(Optional<AnalyticsAnswer> last, AnalyticsAnswer alternative, final MiruSolutionLog solutionLog) {
        return last.or(alternative);
    }

}
