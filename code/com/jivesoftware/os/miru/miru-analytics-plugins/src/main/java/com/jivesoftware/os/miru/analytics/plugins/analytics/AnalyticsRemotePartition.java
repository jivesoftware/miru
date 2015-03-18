package com.jivesoftware.os.miru.analytics.plugins.analytics;

import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.plugin.solution.MiruRemotePartition;
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

    @Override
    public String getEndpoint(MiruPartitionId partitionId) {
        return ANALYTICS_PREFIX + CUSTOM_QUERY_ENDPOINT + "/" + partitionId.getId();
    }

    @Override
    public Class<AnalyticsAnswer> getAnswerClass() {
        return AnalyticsAnswer.class;
    }

    @Override
    public AnalyticsAnswer getEmptyResults() {
        return AnalyticsAnswer.EMPTY_RESULTS;
    }

    @Override
    public EndPointMetrics getEndpointMetrics() {
        return endPointMetrics;
    }

}
