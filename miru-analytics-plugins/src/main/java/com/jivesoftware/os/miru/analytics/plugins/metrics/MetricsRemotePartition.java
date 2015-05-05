package com.jivesoftware.os.miru.analytics.plugins.metrics;

import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.plugin.solution.MiruRemotePartition;
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

    @Override
    public String getEndpoint(MiruPartitionId partitionId) {
        return METRICS_PREFIX + CUSTOM_QUERY_ENDPOINT + "/" + partitionId.getId();
    }

    @Override
    public Class<MetricsAnswer> getAnswerClass() {
        return MetricsAnswer.class;
    }

    @Override
    public MetricsAnswer getEmptyResults() {
        return MetricsAnswer.EMPTY_RESULTS;
    }

    @Override
    public EndPointMetrics getEndpointMetrics() {
        return endPointMetrics;
    }

}
