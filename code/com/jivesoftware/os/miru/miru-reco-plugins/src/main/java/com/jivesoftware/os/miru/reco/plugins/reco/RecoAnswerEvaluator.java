package com.jivesoftware.os.miru.reco.plugins.reco;

import com.jivesoftware.os.miru.plugin.solution.MiruAnswerEvaluator;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;

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
        solutionLog.log("Evaluate partitionsVisited {} < {}", answer.partitionsVisited, 2);
        if (answer.partitionsVisited < 2) { // TODO expose to query or config?
            return false;
        }
        solutionLog.log("Evaluate results size {} >= {}", answer.results.size(), query.desiredNumberOfDistincts);
        return answer.results.size() >= query.desiredNumberOfDistincts;
    }
}
