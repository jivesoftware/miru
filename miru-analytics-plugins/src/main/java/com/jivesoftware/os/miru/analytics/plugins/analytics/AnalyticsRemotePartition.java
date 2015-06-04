package com.jivesoftware.os.miru.analytics.plugins.analytics;

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

import static com.jivesoftware.os.miru.analytics.plugins.analytics.AnalyticsConstants.ANALYTICS_PREFIX;
import static com.jivesoftware.os.miru.analytics.plugins.analytics.AnalyticsConstants.CUSTOM_QUERY_ENDPOINT;

/**
 *
 */
public class AnalyticsRemotePartition implements MiruRemotePartition<AnalyticsQuery, AnalyticsAnswer, AnalyticsReport> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private static final EndPointMetrics endPointMetrics = new EndPointMetrics("process", LOG);

    private final MiruRemotePartitionReader remotePartitionReader;

    public AnalyticsRemotePartition(MiruRemotePartitionReader remotePartitionReader) {
        this.remotePartitionReader = remotePartitionReader;
    }

    private String getEndpoint(MiruPartitionId partitionId) {
        return ANALYTICS_PREFIX + CUSTOM_QUERY_ENDPOINT + "/" + partitionId.getId();
    }

    @Override
    public MiruPartitionResponse<AnalyticsAnswer> askRemote(HttpClient httpClient,
        MiruPartitionId partitionId,
        MiruRequest<AnalyticsQuery> request,
        Optional<AnalyticsReport> report) throws MiruQueryServiceException {
        return remotePartitionReader.read(httpClient,
            getEndpoint(partitionId),
            request,
            AnalyticsAnswer.class,
            report,
            endPointMetrics,
            AnalyticsAnswer.EMPTY_RESULTS);
    }

}
