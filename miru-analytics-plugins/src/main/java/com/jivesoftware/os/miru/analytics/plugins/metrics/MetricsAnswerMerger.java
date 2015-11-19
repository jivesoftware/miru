package com.jivesoftware.os.miru.analytics.plugins.metrics;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.plugin.solution.MiruAnswerMerger;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.plugin.solution.Waveform;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class MetricsAnswerMerger implements MiruAnswerMerger<MetricsAnswer> {

    private final MiruTimeRange timeRange;
    private final int divideTimeRangeIntoNSegments;

    public MetricsAnswerMerger(MiruTimeRange timeRange, int divideTimeRangeIntoNSegments) {
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
    public MetricsAnswer merge(Optional<MetricsAnswer> last, MetricsAnswer currentAnswer, MiruSolutionLog solutionLog) {
        if (!last.isPresent()) {
            solutionLog.log(MiruSolutionLogLevel.INFO, "merge: no last answer, using current answer.");
            return currentAnswer;
        }

        List<Waveform> mergedWaveforms;
        MetricsAnswer lastAnswer = last.get();
        if (currentAnswer.waveforms == null) {
            if (lastAnswer.waveforms == null) {
                solutionLog.log(MiruSolutionLogLevel.WARN, "merge: current and last waveforms are null.");
                mergedWaveforms = null;
            } else {
                solutionLog.log(MiruSolutionLogLevel.WARN, "merge: current waveforms are null, using last answer.");
                mergedWaveforms = lastAnswer.waveforms;
            }
        } else {

            List<Waveform> biggerList = lastAnswer.waveforms.size() > currentAnswer.waveforms.size() ? lastAnswer.waveforms : currentAnswer.waveforms;
            Map<String, Waveform> smallerSet = lastAnswer.waveforms.size() > currentAnswer.waveforms.size()
                ? toMap(currentAnswer.waveforms)
                : toMap(lastAnswer.waveforms);

            mergeWaveform(smallerSet, biggerList, solutionLog);
            mergedWaveforms = biggerList;
            solutionLog.log(MiruSolutionLogLevel.INFO, "merge: merged last answer size={}, with current answer size={}.",
                lastAnswer.waveforms.size(), currentAnswer.waveforms.size());
        }

        return new MetricsAnswer(mergedWaveforms, currentAnswer.resultsExhausted);

    }

    private Map<String, Waveform> toMap(List<Waveform> waveforms) {
        Map<String, Waveform> map = Maps.newHashMapWithExpectedSize(waveforms.size());
        for (Waveform waveform : waveforms) {
            map.put(waveform.getId(), waveform);
        }
        return map;
    }

    private void mergeWaveform(Map<String, Waveform> mergedWaveforms, List<Waveform> waveforms, MiruSolutionLog solutionLog) {
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
    public MetricsAnswer done(Optional<MetricsAnswer> last, MetricsAnswer alternative, final MiruSolutionLog solutionLog) {
        return last.or(alternative);
    }

}
