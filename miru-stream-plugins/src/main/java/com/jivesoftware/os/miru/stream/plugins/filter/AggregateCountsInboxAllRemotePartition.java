package com.jivesoftware.os.miru.stream.plugins.filter;

import com.google.common.base.Optional;
import com.jivesoftware.os.jive.utils.http.client.HttpClient;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.plugin.solution.MiruPartitionResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruRemotePartition;
import com.jivesoftware.os.miru.plugin.solution.MiruRemotePartitionReader;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.mlogger.core.EndPointMetrics;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;

import static com.jivesoftware.os.miru.stream.plugins.filter.AggregateCountsConstants.FILTER_PREFIX;
import static com.jivesoftware.os.miru.stream.plugins.filter.AggregateCountsConstants.INBOX_ALL_QUERY_ENDPOINT;

/**
 *
 */
public class AggregateCountsInboxAllRemotePartition implements MiruRemotePartition<AggregateCountsQuery, AggregateCountsAnswer, AggregateCountsReport> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private static final EndPointMetrics endPointMetrics = new EndPointMetrics("process", LOG);

    private final MiruRemotePartitionReader remotePartitionReader;

    public AggregateCountsInboxAllRemotePartition(MiruRemotePartitionReader remotePartitionReader) {
        this.remotePartitionReader = remotePartitionReader;
    }

    private String getEndpoint(MiruPartitionId partitionId) {
        return FILTER_PREFIX + INBOX_ALL_QUERY_ENDPOINT + "/" + partitionId.getId();
    }

    @Override
    public MiruPartitionResponse<AggregateCountsAnswer> askRemote(HttpClient httpClient,
        MiruPartitionId partitionId,
        MiruRequest<AggregateCountsQuery> request,
        Optional<AggregateCountsReport> report) throws MiruQueryServiceException {
        return remotePartitionReader.read(httpClient,
            getEndpoint(partitionId),
            request,
            AggregateCountsAnswer.class,
            report,
            endPointMetrics,
            AggregateCountsAnswer.EMPTY_RESULTS);
    }

}
