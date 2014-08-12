package com.jivesoftware.os.miru.service.stream.factory;

import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.query.TrendingQuery;
import com.jivesoftware.os.miru.api.query.result.TrendingResult;

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
