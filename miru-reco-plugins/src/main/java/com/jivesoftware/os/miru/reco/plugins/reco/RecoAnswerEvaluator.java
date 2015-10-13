package com.jivesoftware.os.miru.reco.plugins.reco;

import com.jivesoftware.os.miru.plugin.solution.MiruAnswerEvaluator;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;

/**
*
*/
public class RecoAnswerEvaluator implements MiruAnswerEvaluator<RecoAnswer> {

    private final RecoQuery query;

    public RecoAnswerEvaluator(RecoQuery query) {
        this.query = query;
    }

    @Override
    public boolean isDone(RecoAnswer answer, MiruSolutionLog solutionLog) {
        solutionLog.log(MiruSolutionLogLevel.INFO, "Results exhausted = {}", answer.resultsExhausted);
        if (answer.resultsExhausted) {
            return true;
        }
        solutionLog.log(MiruSolutionLogLevel.INFO, "Evaluate partitionsVisited {} < {}", answer.partitionsVisited, 2);
        if (answer.partitionsVisited < 2) { // TODO expose to query or config?
            return false;
        }
        solutionLog.log(MiruSolutionLogLevel.INFO, "Evaluate results size {} >= {}", answer.results.size(), query.desiredNumberOfDistincts);
        return answer.results.size() >= query.desiredNumberOfDistincts;
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
