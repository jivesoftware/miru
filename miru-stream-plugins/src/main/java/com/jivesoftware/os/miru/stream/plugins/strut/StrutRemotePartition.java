package com.jivesoftware.os.miru.stream.plugins.strut;

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

/**
 *
 */
public class StrutRemotePartition implements MiruRemotePartition<StrutQuery, StrutAnswer, StrutReport> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private static final EndPointMetrics endPointMetrics = new EndPointMetrics("process", LOG);

    private final MiruRemotePartitionReader remotePartitionReader;

    public StrutRemotePartition(MiruRemotePartitionReader remotePartitionReader) {
        this.remotePartitionReader = remotePartitionReader;
    }

    private String getEndpoint(MiruPartitionId partitionId) {
        return StrutConstants.STRUT_PREFIX + StrutConstants.CUSTOM_QUERY_ENDPOINT + "/" + partitionId.getId();
    }

    @Override
    public MiruPartitionResponse<StrutAnswer> askRemote(MiruHost host,
        MiruPartitionId partitionId,
        MiruRequest<StrutQuery> request,
        Optional<StrutReport> report) throws MiruQueryServiceException {
        return remotePartitionReader.read("strut",
            host,
            getEndpoint(partitionId),
            request,
            StrutAnswer.class,
            report,
            endPointMetrics,
            StrutAnswer.EMPTY_RESULTS);
    }
}
