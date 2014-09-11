package com.jivesoftware.os.miru.reco.plugins.reco;

import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.query.solution.MiruAnswerEvaluator;

/**
*
*/
public class RecoAnswerEvaluator implements MiruAnswerEvaluator<RecoAnswer> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final RecoQuery query;

    public RecoAnswerEvaluator(RecoQuery query) {
        this.query = query;
    }

    @Override
    public boolean isDone(RecoAnswer answer) {
        log.debug("Evaluate {} >= {}", answer.results.size(), query.desiredNumberOfDistincts);
        return answer.results.size() >= query.desiredNumberOfDistincts; // TODO fix, this exits too fast!
    }
}
