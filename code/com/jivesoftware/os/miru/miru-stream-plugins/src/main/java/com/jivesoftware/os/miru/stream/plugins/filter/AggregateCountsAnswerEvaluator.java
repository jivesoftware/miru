package com.jivesoftware.os.miru.stream.plugins.filter;

import com.jivesoftware.os.miru.query.solution.MiruAnswerEvaluator;
import com.jivesoftware.os.miru.query.solution.MiruSolutionLog;

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
}
