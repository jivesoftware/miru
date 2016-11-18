package com.jivesoftware.os.miru.stream.plugins.fulltext;

import com.jivesoftware.os.miru.plugin.solution.MiruAnswerEvaluator;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;

/**
 *
 */
public class FullTextAnswerEvaluator implements MiruAnswerEvaluator<FullTextAnswer> {

    private final FullTextQuery query;

    public FullTextAnswerEvaluator(FullTextQuery query) {
        this.query = query;
    }

    @Override
    public boolean isDone(FullTextAnswer answer, MiruSolutionLog solutionLog) {
        if (query.strategy == FullTextQuery.Strategy.TF_IDF) {
            return false;
        }

        solutionLog.log(MiruSolutionLogLevel.INFO, "Results exhausted = {}", answer.resultsExhausted);
        if (answer.resultsExhausted) {
            return true;
        }
        solutionLog.log(MiruSolutionLogLevel.INFO, "Evaluate {} >= {}", answer.results.size(), query.desiredNumberOfResults);
        return false;  //answer.results.size() >= query.desiredNumberOfResults;
    }

    @Override
    public boolean stopOnUnsolvablePartition() {
        return (query.strategy == FullTextQuery.Strategy.TIME);
    }

    @Override
    public boolean useParallelSolver() {
        return true; //(query.strategy == FullTextQuery.Strategy.TF_IDF);
    }
}
