package com.jivesoftware.os.miru.anomaly.plugins;

import com.jivesoftware.os.miru.plugin.solution.MiruAnswerEvaluator;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;

/**
 *
 */
public class AnomalyAnswerEvaluator implements MiruAnswerEvaluator<AnomalyAnswer> {

    @Override
    public boolean isDone(AnomalyAnswer answer, MiruSolutionLog solutionLog) {
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
