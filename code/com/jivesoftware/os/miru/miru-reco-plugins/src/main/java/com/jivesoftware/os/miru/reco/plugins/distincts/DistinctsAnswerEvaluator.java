package com.jivesoftware.os.miru.reco.plugins.distincts;

import com.jivesoftware.os.miru.plugin.solution.MiruAnswerEvaluator;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;

/**
 *
 */
public class DistinctsAnswerEvaluator implements MiruAnswerEvaluator<DistinctsAnswer> {

    @Override
    public boolean isDone(DistinctsAnswer answer, MiruSolutionLog solutionLog) {
        solutionLog.log("Results exhausted = {}", answer.resultsExhausted);
        return answer.resultsExhausted;
    }

    @Override
    public boolean stopOnUnsolvablePartition() {
        return false;
    }
}
