package com.jivesoftware.os.miru.stream.plugins.filter;

import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.query.solution.MiruAnswerEvaluator;

/**
*
*/
public class AggregateCountsAnswerEvaluator implements MiruAnswerEvaluator<AggregateCountsAnswer> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final AggregateCountsQuery query;

    public AggregateCountsAnswerEvaluator(AggregateCountsQuery query) {
        this.query = query;
    }

    @Override
    public boolean isDone(AggregateCountsAnswer answer) {
        int requiredDistincts = query.desiredNumberOfDistincts + query.startFromDistinctN;
        log.debug("Evaluate {} >= {}", answer.collectedDistincts, requiredDistincts);
        return answer.collectedDistincts >= requiredDistincts;
    }
}
