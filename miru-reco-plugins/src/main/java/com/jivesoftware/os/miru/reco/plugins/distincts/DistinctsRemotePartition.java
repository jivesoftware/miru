package com.jivesoftware.os.miru.reco.plugins.distincts;

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

import static com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsConstants.CUSTOM_QUERY_ENDPOINT;
import static com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsConstants.DISTINCTS_PREFIX;

/**
 *
 */
public class DistinctsRemotePartition implements MiruRemotePartition<DistinctsQuery, DistinctsAnswer, DistinctsReport> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private static final EndPointMetrics endPointMetrics = new EndPointMetrics("process", LOG);

    private final MiruRemotePartitionReader remotePartitionReader;

    public DistinctsRemotePartition(MiruRemotePartitionReader remotePartitionReader) {
        this.remotePartitionReader = remotePartitionReader;
    }

    private String getEndpoint(MiruPartitionId partitionId) {
        return DISTINCTS_PREFIX + CUSTOM_QUERY_ENDPOINT + "/" + partitionId.getId();
    }

    @Override
    public MiruPartitionResponse<DistinctsAnswer> askRemote(MiruHost host,
        MiruPartitionId partitionId,
        MiruRequest<DistinctsQuery> request,
        Optional<DistinctsReport> report) throws MiruQueryServiceException {
        return remotePartitionReader.read("distincts",
            host,
            getEndpoint(partitionId),
            request,
            DistinctsAnswer.class,
            report,
            endPointMetrics,
            DistinctsAnswer.EMPTY_RESULTS);
    }
}
