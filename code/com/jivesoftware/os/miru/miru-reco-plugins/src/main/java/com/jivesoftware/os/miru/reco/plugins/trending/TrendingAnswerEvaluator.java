package com.jivesoftware.os.miru.reco.plugins.trending;

import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.query.MiruAnswerEvaluator;

/**
 *
 */
public class TrendingAnswerEvaluator implements MiruAnswerEvaluator<TrendingAnswer> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    @Override
    public boolean isDone(TrendingAnswer result) {
        log.debug("Results exhausted = {}", result.resultsExhausted);
        return result.resultsExhausted;
    }
}
