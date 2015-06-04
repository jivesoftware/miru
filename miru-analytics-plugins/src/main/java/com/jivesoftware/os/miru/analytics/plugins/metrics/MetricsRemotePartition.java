package com.jivesoftware.os.miru.analytics.plugins.metrics;

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

import static com.jivesoftware.os.miru.analytics.plugins.metrics.MetricsConstants.CUSTOM_QUERY_ENDPOINT;
import static com.jivesoftware.os.miru.analytics.plugins.metrics.MetricsConstants.METRICS_PREFIX;

/**
 *
 */
public class MetricsRemotePartition implements MiruRemotePartition<MetricsQuery, MetricsAnswer, MetricsReport> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private static final EndPointMetrics endPointMetrics = new EndPointMetrics("process", LOG);

    private final MiruRemotePartitionReader remotePartitionReader;

    public MetricsRemotePartition(MiruRemotePartitionReader remotePartitionReader) {
        this.remotePartitionReader = remotePartitionReader;
    }

    private String getEndpoint(MiruPartitionId partitionId) {
        return METRICS_PREFIX + CUSTOM_QUERY_ENDPOINT + "/" + partitionId.getId();
    }

    @Override
    public MiruPartitionResponse<MetricsAnswer> askRemote(HttpClient httpClient,
        MiruPartitionId partitionId,
        MiruRequest<MetricsQuery> request,
        Optional<MetricsReport> report) throws MiruQueryServiceException {
        return remotePartitionReader.read(httpClient,
            getEndpoint(partitionId),
            request,
            MetricsAnswer.class,
            report,
            endPointMetrics,
            MetricsAnswer.EMPTY_RESULTS);
    }

}
