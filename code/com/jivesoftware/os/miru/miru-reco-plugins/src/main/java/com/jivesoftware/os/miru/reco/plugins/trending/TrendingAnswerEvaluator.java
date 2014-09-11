package com.jivesoftware.os.miru.reco.plugins.trending;

import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.query.solution.MiruAnswerEvaluator;

/**
 *
 */
public class TrendingAnswerEvaluator implements MiruAnswerEvaluator<TrendingAnswer> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    @Override
    public boolean isDone(TrendingAnswer answer) {
        log.debug("Results exhausted = {}", answer.resultsExhausted);
        return answer.resultsExhausted;
    }
}
