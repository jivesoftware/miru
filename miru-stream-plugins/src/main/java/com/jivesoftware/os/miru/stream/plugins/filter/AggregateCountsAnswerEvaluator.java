package com.jivesoftware.os.miru.stream.plugins.filter;

import com.jivesoftware.os.miru.plugin.solution.MiruAnswerEvaluator;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;

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
        solutionLog.log(MiruSolutionLogLevel.INFO, "Results exhausted = {}", answer.resultsExhausted);
        if (answer.resultsExhausted) {
            return true;
        }
        int requiredDistincts = query.desiredNumberOfDistincts + query.startFromDistinctN;
        solutionLog.log(MiruSolutionLogLevel.INFO, "Evaluate {} >= {}", answer.collectedDistincts, requiredDistincts);
        return answer.collectedDistincts >= requiredDistincts;
    }

    @Override
    public boolean stopOnUnsolvablePartition() {
        return true;
    }
}
