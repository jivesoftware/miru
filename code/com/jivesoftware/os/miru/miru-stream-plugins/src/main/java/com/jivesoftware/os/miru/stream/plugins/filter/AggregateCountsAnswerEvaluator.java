package com.jivesoftware.os.miru.stream.plugins.filter;

import com.jivesoftware.os.miru.plugin.solution.MiruAnswerEvaluator;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;

/**
*
*/
public class AggregateCountsAnswerEvaluator implements MiruAnswerEvaluator<AggregateCountsAnswer> {

    private final AggregateCountsQuery query;

    public AggregateCountsAnswerEvaluator(AggregateCountsQuery query) {
        this.query = query;
    }

    @Override
    public boolean isDone(AggregateCountsAnswer answer, MiruSolutionLog solutionLog) {
        int requiredDistincts = query.desiredNumberOfDistincts + query.startFromDistinctN;
        solutionLog.log("Evaluate {} >= {}", answer.collectedDistincts, requiredDistincts);
        return answer.collectedDistincts >= requiredDistincts;
    }

    @Override
    public boolean stopOnUnsolvablePartition() {
        return true;
    }
}
