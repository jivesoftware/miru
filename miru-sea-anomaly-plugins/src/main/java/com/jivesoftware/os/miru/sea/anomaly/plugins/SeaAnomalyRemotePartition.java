package com.jivesoftware.os.miru.sea.anomaly.plugins;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.plugin.solution.MiruPartitionResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruRemotePartition;
import com.jivesoftware.os.miru.plugin.solution.MiruRemotePartitionReader;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
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

    private final MiruRemotePartitionReader remotePartitionReader;

    public SeaAnomalyRemotePartition(MiruRemotePartitionReader remotePartitionReader) {
        this.remotePartitionReader = remotePartitionReader;
    }

    private String getEndpoint(MiruPartitionId partitionId) {
        return SeaAnomalyConstants.SEA_ANOMALY_PREFIX + CUSTOM_QUERY_ENDPOINT + "/" + partitionId.getId();
    }

    @Override
    public MiruPartitionResponse<SeaAnomalyAnswer> askRemote(MiruHost host,
        MiruPartitionId partitionId,
        MiruRequest<SeaAnomalyQuery> request,
        Optional<SeaAnomalyReport> report) throws MiruQueryServiceException {
        return remotePartitionReader.read("anomaly",
            host,
            getEndpoint(partitionId),
            request,
            SeaAnomalyAnswer.class,
            report,
            endPointMetrics,
            SeaAnomalyAnswer.EMPTY_RESULTS);
    }

}
