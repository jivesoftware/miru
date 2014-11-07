package com.jivesoftware.os.miru.analytics.plugins.analytics;

import com.jivesoftware.os.miru.plugin.solution.MiruAnswerEvaluator;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;

/**
 *
 */
public class AnalyticsAnswerEvaluator implements MiruAnswerEvaluator<AnalyticsAnswer> {

    @Override
    public boolean isDone(AnalyticsAnswer answer, MiruSolutionLog solutionLog) {
        solutionLog.log("Results exhausted = {}", answer.resultsExhausted);
        return answer.resultsExhausted;
    }
}
