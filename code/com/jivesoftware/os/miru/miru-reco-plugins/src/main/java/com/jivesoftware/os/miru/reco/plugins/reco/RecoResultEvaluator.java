package com.jivesoftware.os.miru.reco.plugins.reco;

import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.query.MiruResultEvaluator;

/**
*
*/
public class RecoResultEvaluator implements MiruResultEvaluator<RecoResult> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final RecoQuery query;

    public RecoResultEvaluator(RecoQuery query) {
        this.query = query;
    }

    @Override
    public boolean isDone(RecoResult result) {
        log.debug("Evaluate {} >= {}", result.results.size(), query.resultCount);
        return result.results.size() >= query.resultCount; // TODO fix this exits to fast!
    }
}
