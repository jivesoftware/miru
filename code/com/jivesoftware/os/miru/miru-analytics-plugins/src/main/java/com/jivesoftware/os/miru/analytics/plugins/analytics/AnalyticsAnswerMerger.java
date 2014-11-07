package com.jivesoftware.os.miru.analytics.plugins.analytics;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.jivesoftware.os.miru.plugin.solution.MiruAnswerMerger;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;

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

        AnalyticsAnswer lastAnswer = last.get();
        int l = currentAnswer.waveform.waveform.length;
        long[] merged = new long[l];
        for (int i = 0; i < l; i++) {
            merged[i] += lastAnswer.waveform.waveform[i];
        }

        for (int i = 0; i < l; i++) {
            merged[i] += currentAnswer.waveform.waveform[i];
        }

        AnalyticsAnswer mergedAnswer = new AnalyticsAnswer(new AnalyticsAnswer.Waveform(merged), currentAnswer.resultsExhausted);
        return mergedAnswer;
    }

    @Override
    public AnalyticsAnswer done(Optional<AnalyticsAnswer> last, AnalyticsAnswer alternative, final MiruSolutionLog solutionLog) {
        return last.transform(new Function<AnalyticsAnswer, AnalyticsAnswer>() {
            @Override
            public AnalyticsAnswer apply(AnalyticsAnswer result) {
                return new AnalyticsAnswer(result.waveform, result.resultsExhausted);
            }
        }).or(alternative);
    }

}
