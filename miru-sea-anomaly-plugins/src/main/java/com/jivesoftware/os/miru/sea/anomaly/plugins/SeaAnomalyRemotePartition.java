package com.jivesoftware.os.miru.sea.anomaly.plugins;

import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.plugin.solution.MiruRemotePartition;
import com.jivesoftware.os.mlogger.core.EndPointMetrics;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;

import static com.jivesoftware.os.miru.sea.anomaly.plugins.SeaAnomalyConstants.CUSTOM_QUERY_ENDPOINT;

/**
 *
 */
public class SeaAnomalyRemotePartition implements MiruRemotePartition<SeaAnomalyQuery, SeaAnomalyAnswer, SeaAnomalyReport> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private static final EndPointMetrics endPointMetrics = new EndPointMetrics("process", LOG);

    @Override
    public String getEndpoint(MiruPartitionId partitionId) {
        return SeaAnomalyConstants.SEA_ANOMALY_PREFIX + CUSTOM_QUERY_ENDPOINT + "/" + partitionId.getId();
    }

    @Override
    public Class<SeaAnomalyAnswer> getAnswerClass() {
        return SeaAnomalyAnswer.class;
    }

    @Override
    public SeaAnomalyAnswer getEmptyResults() {
        return SeaAnomalyAnswer.EMPTY_RESULTS;
    }

    @Override
    public EndPointMetrics getEndpointMetrics() {
        return endPointMetrics;
    }

}
