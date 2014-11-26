package com.jivesoftware.os.miru.stream.plugins.count;

import com.jivesoftware.os.miru.plugin.solution.MiruAnswerEvaluator;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;

/**
*
*/
public class DistinctCountAnswerEvaluator implements MiruAnswerEvaluator<DistinctCountAnswer> {

    private final DistinctCountQuery query;

    public DistinctCountAnswerEvaluator(DistinctCountQuery query) {
        this.query = query;
    }

    @Override
    public boolean isDone(DistinctCountAnswer answer, MiruSolutionLog solutionLog) {
        solutionLog.log("Evaluate {} >= {}", answer.collectedDistincts, query.desiredNumberOfDistincts);
        return answer.collectedDistincts >= query.desiredNumberOfDistincts;
    }

    @Override
    public boolean stopOnUnsolvablePartition() {
        return true;
    }
}
