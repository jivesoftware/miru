package com.jivesoftware.os.miru.reco.plugins.distincts;

import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.plugin.solution.MiruRemotePartition;
import com.jivesoftware.os.mlogger.core.EndPointMetrics;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;

import static com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsConstants.CUSTOM_QUERY_ENDPOINT;
import static com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsConstants.DISTINCTS_PREFIX;

/**
 *
 */
public class DistinctsRemotePartition implements MiruRemotePartition<DistinctsQuery, DistinctsAnswer, DistinctsReport> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private static final EndPointMetrics endPointMetrics = new EndPointMetrics("process", LOG);

    @Override
    public String getEndpoint(MiruPartitionId partitionId) {
        return DISTINCTS_PREFIX + CUSTOM_QUERY_ENDPOINT + "/" + partitionId.getId();
    }

    @Override
    public Class<DistinctsAnswer> getAnswerClass() {
        return DistinctsAnswer.class;
    }

    @Override
    public DistinctsAnswer getEmptyResults() {
        return DistinctsAnswer.EMPTY_RESULTS;
    }

    @Override
    public EndPointMetrics getEndpointMetrics() {
        return endPointMetrics;
    }

}
