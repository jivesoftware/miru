package com.jivesoftware.os.miru.stream.plugins.fulltext;

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
import com.jivesoftware.os.routing.bird.http.client.HttpClient;

import static com.jivesoftware.os.miru.stream.plugins.fulltext.FullTextConstants.CUSTOM_QUERY_ENDPOINT;
import static com.jivesoftware.os.miru.stream.plugins.fulltext.FullTextConstants.FULLTEXT_PREFIX;

/**
 *
 */
public class FullTextCustomRemotePartition implements MiruRemotePartition<FullTextQuery, FullTextAnswer, FullTextReport> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private static final EndPointMetrics endPointMetrics = new EndPointMetrics("process", LOG);

    private final MiruRemotePartitionReader remotePartitionReader;

    public FullTextCustomRemotePartition(MiruRemotePartitionReader remotePartitionReader) {
        this.remotePartitionReader = remotePartitionReader;
    }

    private String getEndpoint(MiruPartitionId partitionId) {
        return FULLTEXT_PREFIX + CUSTOM_QUERY_ENDPOINT + "/" + partitionId.getId();
    }

    @Override
    public MiruPartitionResponse<FullTextAnswer> askRemote(MiruHost host,
        MiruPartitionId partitionId,
        MiruRequest<FullTextQuery> request,
        Optional<FullTextReport> report) throws MiruQueryServiceException {
        return remotePartitionReader.read(host, getEndpoint(partitionId), request, FullTextAnswer.class, report, endPointMetrics,
            FullTextAnswer.EMPTY_RESULTS);
    }
}
