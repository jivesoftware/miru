package com.jivesoftware.os.miru.analytics.plugins.metrics;

import com.jivesoftware.os.miru.plugin.solution.MiruAnswerEvaluator;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;

/**
 *
 */
public class MetricsAnswerEvaluator implements MiruAnswerEvaluator<MetricsAnswer> {

    @Override
    public boolean isDone(MetricsAnswer answer, MiruSolutionLog solutionLog) {
        solutionLog.log(MiruSolutionLogLevel.INFO, "Results exhausted = {}", answer.resultsExhausted);
        return answer.resultsExhausted;
    }

    @Override
    public boolean stopOnUnsolvablePartition() {
        return false;
    }

    @Override
    public boolean useParallelSolver() {
        return false;
    }
}
