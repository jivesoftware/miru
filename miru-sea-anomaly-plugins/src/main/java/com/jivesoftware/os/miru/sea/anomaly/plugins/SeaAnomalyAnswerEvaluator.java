package com.jivesoftware.os.miru.sea.anomaly.plugins;

import com.jivesoftware.os.miru.plugin.solution.MiruAnswerEvaluator;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;

/**
 *
 */
public class SeaAnomalyAnswerEvaluator implements MiruAnswerEvaluator<SeaAnomalyAnswer> {

    @Override
    public boolean isDone(SeaAnomalyAnswer answer, MiruSolutionLog solutionLog) {
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
