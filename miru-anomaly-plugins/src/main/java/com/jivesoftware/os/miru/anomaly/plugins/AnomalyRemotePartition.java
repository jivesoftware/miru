package com.jivesoftware.os.miru.anomaly.plugins;

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

import static com.jivesoftware.os.miru.anomaly.plugins.AnomalyConstants.CUSTOM_QUERY_ENDPOINT;

/**
 *
 */
public class AnomalyRemotePartition implements MiruRemotePartition<AnomalyQuery, AnomalyAnswer, AnomalyReport> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private static final EndPointMetrics endPointMetrics = new EndPointMetrics("process", LOG);

    private final MiruRemotePartitionReader remotePartitionReader;

    public AnomalyRemotePartition(MiruRemotePartitionReader remotePartitionReader) {
        this.remotePartitionReader = remotePartitionReader;
    }

    private String getEndpoint(MiruPartitionId partitionId) {
        return AnomalyConstants.ANOMALY_PREFIX + CUSTOM_QUERY_ENDPOINT + "/" + partitionId.getId();
    }

    @Override
    public MiruPartitionResponse<AnomalyAnswer> askRemote(MiruHost host,
        MiruPartitionId partitionId,
        MiruRequest<AnomalyQuery> request,
        Optional<AnomalyReport> report) throws MiruQueryServiceException {
        return remotePartitionReader.read("anomaly",
            host,
            getEndpoint(partitionId),
            request,
            AnomalyAnswer.class,
            report,
            endPointMetrics,
            AnomalyAnswer.EMPTY_RESULTS);
    }

}
