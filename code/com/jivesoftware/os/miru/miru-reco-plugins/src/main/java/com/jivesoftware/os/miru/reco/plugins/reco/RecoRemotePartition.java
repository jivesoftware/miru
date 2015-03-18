package com.jivesoftware.os.miru.reco.plugins.reco;

import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.plugin.solution.MiruRemotePartition;
import com.jivesoftware.os.mlogger.core.EndPointMetrics;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;

import static com.jivesoftware.os.miru.reco.plugins.reco.RecoConstants.CUSTOM_QUERY_ENDPOINT;
import static com.jivesoftware.os.miru.reco.plugins.reco.RecoConstants.RECO_PREFIX;

/**
 *
 */
public class RecoRemotePartition implements MiruRemotePartition<RecoQuery, RecoAnswer, RecoReport> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private static final EndPointMetrics endPointMetrics = new EndPointMetrics("process", LOG);

    @Override
    public String getEndpoint(MiruPartitionId partitionId) {
        return RECO_PREFIX + CUSTOM_QUERY_ENDPOINT + "/" + partitionId.getId();
    }

    @Override
    public Class<RecoAnswer> getAnswerClass() {
        return RecoAnswer.class;
    }

    @Override
    public RecoAnswer getEmptyResults() {
        return RecoAnswer.EMPTY_RESULTS;
    }

    @Override
    public EndPointMetrics getEndpointMetrics() {
        return endPointMetrics;
    }

}
