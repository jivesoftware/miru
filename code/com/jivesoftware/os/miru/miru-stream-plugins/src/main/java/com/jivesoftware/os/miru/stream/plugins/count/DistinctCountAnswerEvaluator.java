package com.jivesoftware.os.miru.stream.plugins.count;

import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.query.solution.MiruAnswerEvaluator;

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
    public boolean isDone(DistinctCountAnswer answer) {
        log.debug("Evaluate {} >= {}", answer.collectedDistincts, query.desiredNumberOfDistincts);
        return answer.collectedDistincts >= query.desiredNumberOfDistincts;
    }
}
