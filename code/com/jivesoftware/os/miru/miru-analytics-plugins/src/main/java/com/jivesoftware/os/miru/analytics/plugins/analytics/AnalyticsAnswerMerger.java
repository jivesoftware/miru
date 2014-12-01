package com.jivesoftware.os.miru.analytics.plugins.analytics;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.api.base.MiruIBA;
import com.jivesoftware.os.miru.plugin.solution.MiruAnswerMerger;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
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
     * @param last the last merge result
     * @param currentAnswer the next result to merge
     * @param solutionLog
     * @return the merged result
     */
    @Override
    public AnalyticsAnswer merge(Optional<AnalyticsAnswer> last, AnalyticsAnswer currentAnswer, MiruSolutionLog solutionLog) {
        if (!last.isPresent()) {
            return currentAnswer;
        }

        Map<MiruIBA, AnalyticsAnswer.Waveform> mergedWaveforms;
        AnalyticsAnswer lastAnswer = last.get();
        if (currentAnswer.waveforms == null) {
            if (lastAnswer.waveforms == null) {
                solutionLog.log("merge: current and last waveforms are null.");
                mergedWaveforms = null;
            } else {
                solutionLog.log("merge: current waveforms are null, using last answer.");
                mergedWaveforms = lastAnswer.waveforms;
            }
        } else {
            mergedWaveforms = Maps.newHashMap();
            mergeWaveform(mergedWaveforms, lastAnswer);
            mergeWaveform(mergedWaveforms, currentAnswer);
        }

        return new AnalyticsAnswer(mergedWaveforms, currentAnswer.resultsExhausted);
    }

    private void mergeWaveform(Map<MiruIBA, AnalyticsAnswer.Waveform> mergedWaveforms, AnalyticsAnswer addAnswer) {
        for (Map.Entry<MiruIBA, AnalyticsAnswer.Waveform> addEntry : addAnswer.waveforms.entrySet()) {
            MiruIBA key = addEntry.getKey();
            AnalyticsAnswer.Waveform addWaveform = addEntry.getValue();
            AnalyticsAnswer.Waveform mergedWaveform = mergedWaveforms.get(key);
            if (mergedWaveform == null) {
                mergedWaveform = new AnalyticsAnswer.Waveform(new long[addWaveform.waveform.length]);
                mergedWaveforms.put(key, mergedWaveform);
            }
            for (int i = 0; i < mergedWaveform.waveform.length; i++) {
                mergedWaveform.waveform[i] += addWaveform.waveform[i];
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
