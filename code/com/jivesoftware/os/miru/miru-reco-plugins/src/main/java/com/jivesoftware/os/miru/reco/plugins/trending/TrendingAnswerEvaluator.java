package com.jivesoftware.os.miru.reco.plugins.trending;

import com.jivesoftware.os.miru.query.solution.MiruAnswerEvaluator;
import com.jivesoftware.os.miru.query.solution.MiruSolutionLog;

/**
 *
 */
public class TrendingAnswerEvaluator implements MiruAnswerEvaluator<TrendingAnswer> {

    @Override
    public boolean isDone(TrendingAnswer answer, MiruSolutionLog solutionLog) {
        solutionLog.log("Results exhausted = {}", answer.resultsExhausted);
        return answer.resultsExhausted;
    }
}
