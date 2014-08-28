package com.jivesoftware.os.miru.stream.plugins.filter;

import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.query.MiruResultEvaluator;

/**
*
*/
public class AggregateCountsResultEvaluator implements MiruResultEvaluator<AggregateCountsResult> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final AggregateCountsQuery query;

    public AggregateCountsResultEvaluator(AggregateCountsQuery query) {
        this.query = query;
    }

    @Override
    public boolean isDone(AggregateCountsResult result) {
        int requiredDistincts = query.desiredNumberOfDistincts + query.startFromDistinctN;
        log.debug("Evaluate {} >= {}", result.collectedDistincts, requiredDistincts);
        return result.collectedDistincts >= requiredDistincts;
    }
}
