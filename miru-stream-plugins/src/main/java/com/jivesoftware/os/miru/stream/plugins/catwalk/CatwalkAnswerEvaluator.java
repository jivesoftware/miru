package com.jivesoftware.os.miru.stream.plugins.catwalk;

import com.jivesoftware.os.miru.plugin.solution.MiruAnswerEvaluator;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;

/**
 *
 */
public class CatwalkAnswerEvaluator implements MiruAnswerEvaluator<CatwalkAnswer> {

    @Override
    public boolean isDone(CatwalkAnswer answer, MiruSolutionLog solutionLog) {
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
