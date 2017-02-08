package com.jivesoftware.os.miru.stream.plugins.strut;

import com.jivesoftware.os.miru.plugin.solution.MiruAnswerEvaluator;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;

/**
 *
 */
public class StrutAnswerEvaluator implements MiruAnswerEvaluator<StrutAnswer> {

    @Override
    public boolean isDone(StrutAnswer answer, MiruSolutionLog solutionLog) {
        solutionLog.log(MiruSolutionLogLevel.INFO, "Results exhausted = {}", answer.resultsExhausted);
        return answer.resultsExhausted;
    }

    @Override
    public boolean stopOnUnsolvablePartition() {
        return true;
    }

    @Override
    public boolean useParallelSolver() {
        return true;
    }
}
