package com.jivesoftware.os.miru.stream.plugins.count;

import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.query.MiruResultEvaluator;

/**
*
*/
public class DistinctCountResultEvaluator implements MiruResultEvaluator<DistinctCountResult> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final DistinctCountQuery query;

    public DistinctCountResultEvaluator(DistinctCountQuery query) {
        this.query = query;
    }

    @Override
    public boolean isDone(DistinctCountResult result) {
        log.debug("Evaluate {} >= {}", result.collectedDistincts, query.desiredNumberOfDistincts);
        return result.collectedDistincts >= query.desiredNumberOfDistincts;
    }
}
