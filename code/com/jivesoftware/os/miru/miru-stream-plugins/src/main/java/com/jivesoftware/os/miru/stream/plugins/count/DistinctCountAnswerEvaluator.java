package com.jivesoftware.os.miru.stream.plugins.count;

import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.query.MiruAnswerEvaluator;

/**
*
*/
public class DistinctCountAnswerEvaluator implements MiruAnswerEvaluator<DistinctCountAnswer> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final DistinctCountQuery query;

    public DistinctCountAnswerEvaluator(DistinctCountQuery query) {
        this.query = query;
    }

    @Override
    public boolean isDone(DistinctCountAnswer result) {
        log.debug("Evaluate {} >= {}", result.collectedDistincts, query.desiredNumberOfDistincts);
        return result.collectedDistincts >= query.desiredNumberOfDistincts;
    }
}
