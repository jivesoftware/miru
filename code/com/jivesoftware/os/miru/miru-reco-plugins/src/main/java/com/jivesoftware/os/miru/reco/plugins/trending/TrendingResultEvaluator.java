package com.jivesoftware.os.miru.reco.plugins.trending;

import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.query.MiruResultEvaluator;

/**
*
*/
public class TrendingResultEvaluator implements MiruResultEvaluator<TrendingResult> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final TrendingQuery query;

    public TrendingResultEvaluator(TrendingQuery query) {
        this.query = query;
    }

    @Override
    public boolean isDone(TrendingResult result) {
        log.debug("Evaluate {} >= {}", result.collectedDistincts, query.desiredNumberOfDistincts);
        return result.collectedDistincts >= query.desiredNumberOfDistincts;
    }
}
