package com.jivesoftware.os.miru.stream.plugins.strut;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.MiruActorId;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.plugin.partition.MiruQueryablePartition;
import com.jivesoftware.os.miru.plugin.partition.OrderedPartitions;
import com.jivesoftware.os.miru.plugin.solution.MiruPartitionResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruRemotePartition;
import com.jivesoftware.os.miru.plugin.solution.MiruRemotePartitionReader;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
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

    public void shareRemote(String queryKey,
        MiruPartitionCoord coord,
        OrderedPartitions<?, ?> orderedPartitions,
        StrutShare share) {
        String endpoint = StrutConstants.STRUT_PREFIX + StrutConstants.SHARE_ENDPOINT;
        for (MiruQueryablePartition<?, ?> partition : orderedPartitions.partitions) {
            if (!partition.isLocal()) {
                MiruPartitionCoord remote = partition.getCoord();
                try {
                    LOG.info("Sharing update for catwalkId:{} scorables:{} with coord:{}", share.catwalkDefinition.catwalkId, share.scorables.size(), remote);
                    remotePartitionReader.read(queryKey,
                        remote.host,
                        endpoint,
                        new MiruRequest<>("strut/share",
                            coord.tenantId,
                            MiruActorId.NOT_PROVIDED,
                            MiruAuthzExpression.NOT_PROVIDED,
                            share,
                            MiruSolutionLogLevel.NONE),
                        String.class,
                        Optional.<String>absent(),
                        endPointMetrics,
                        null);
                } catch (MiruQueryServiceException e) {
                    LOG.warn("Failed to share strut updates with {}", new Object[] { remote }, e);
                }
            }
        }

    }
}
