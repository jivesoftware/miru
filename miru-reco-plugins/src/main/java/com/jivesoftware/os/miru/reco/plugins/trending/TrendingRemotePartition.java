package com.jivesoftware.os.miru.reco.plugins.trending;

import com.google.common.base.Optional;
import com.jivesoftware.os.jive.utils.http.client.HttpClient;
import com.jivesoftware.os.miru.analytics.plugins.analytics.AnalyticsAnswer;
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
public class TrendingRemotePartition implements MiruRemotePartition<TrendingQuery, AnalyticsAnswer, TrendingReport> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private static final EndPointMetrics endPointMetrics = new EndPointMetrics("process", LOG);

    private final MiruRemotePartitionReader remotePartitionReader;

    public TrendingRemotePartition(MiruRemotePartitionReader remotePartitionReader) {
        this.remotePartitionReader = remotePartitionReader;
    }

    private String getEndpoint(MiruPartitionId partitionId) {
        return TrendingConstants.TRENDING_PREFIX + TrendingConstants.CUSTOM_QUERY_ENDPOINT + "/" + partitionId.getId();
    }

    @Override
    public MiruPartitionResponse<AnalyticsAnswer> askRemote(HttpClient httpClient,
        MiruPartitionId partitionId,
        MiruRequest<TrendingQuery> request,
        Optional<TrendingReport> report) throws MiruQueryServiceException {
        return remotePartitionReader.read(httpClient,
            getEndpoint(partitionId),
            request,
            AnalyticsAnswer.class,
            report,
            endPointMetrics,
            AnalyticsAnswer.EMPTY_RESULTS);
    }

}
