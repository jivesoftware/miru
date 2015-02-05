package com.jivesoftware.os.miru.analytics.plugins.metrics;

import com.google.common.base.Optional;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.plugin.solution.MiruPartitionResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruRequestAndReport;
import com.jivesoftware.os.mlogger.core.EndPointMetrics;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;

import static com.jivesoftware.os.miru.analytics.plugins.metrics.MetricsConstants.CUSTOM_QUERY_ENDPOINT;
import static com.jivesoftware.os.miru.analytics.plugins.metrics.MetricsConstants.METRICS_PREFIX;

/**
 *
 */
public class MetricsRemotePartitionReader {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final EndPointMetrics processMetrics;
    private final RequestHelper requestHelper;

    public MetricsRemotePartitionReader(RequestHelper requestHelper) {
        this.requestHelper = requestHelper;
        this.processMetrics = new EndPointMetrics("process", LOG);
    }

    public MiruPartitionResponse<MetricsAnswer> scoreMetricing(MiruPartitionId partitionId, MiruRequest<MetricsQuery> request,
        Optional<MetricsReport> report) throws MiruQueryServiceException {

        MiruRequestAndReport<MetricsQuery, MetricsReport> params = new MiruRequestAndReport<>(request, report.orNull());
        processMetrics.start();
        try {
            return requestHelper.executeRequest(params,
                METRICS_PREFIX + CUSTOM_QUERY_ENDPOINT + "/" + partitionId.getId(),
                MiruPartitionResponse.class, new Class[]{MetricsAnswer.class}, new MiruPartitionResponse<>(MetricsAnswer.EMPTY_RESULTS, null));
        } catch (RuntimeException e) {
            throw new MiruQueryServiceException("Failed score metrics for partition: " + partitionId.getId(), e);
        } finally {
            processMetrics.stop();
        }
    }

}
