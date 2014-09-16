package com.jivesoftware.os.miru.reco.plugins.reco;

import com.jivesoftware.os.miru.query.solution.MiruAnswerEvaluator;
import com.jivesoftware.os.miru.query.solution.MiruSolutionLog;

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
        solutionLog.log("Evaluate {} >= {}", answer.results.size(), query.desiredNumberOfDistincts);
        return answer.results.size() >= query.desiredNumberOfDistincts; // TODO fix, this exits too fast!
    }
}
