package com.jivesoftware.os.miru.analytics.plugins.analytics;

import com.google.common.base.Optional;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.jive.utils.logger.EndPointMetrics;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.plugin.solution.MiruPartitionResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruRequestAndReport;

import static com.jivesoftware.os.miru.analytics.plugins.analytics.AnalyticsConstants.ANALYTICS_PREFIX;
import static com.jivesoftware.os.miru.analytics.plugins.analytics.AnalyticsConstants.CUSTOM_QUERY_ENDPOINT;

/**
 *
 */
public class AnalyticsRemotePartitionReader {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final EndPointMetrics processMetrics;
    private final RequestHelper requestHelper;

    public AnalyticsRemotePartitionReader(RequestHelper requestHelper) {
        this.requestHelper = requestHelper;
        this.processMetrics = new EndPointMetrics("process", LOG);
    }

    public MiruPartitionResponse<AnalyticsAnswer> scoreAnalyticing(MiruPartitionId partitionId, MiruRequest<AnalyticsQuery> request,
        Optional<AnalyticsReport> report) throws MiruQueryServiceException {

        MiruRequestAndReport<AnalyticsQuery, AnalyticsReport> params = new MiruRequestAndReport<>(request, report.orNull());
        processMetrics.start();
        try {
            return requestHelper.executeRequest(params,
                ANALYTICS_PREFIX + CUSTOM_QUERY_ENDPOINT + "/" + partitionId.getId(),
                MiruPartitionResponse.class, new Class[]{AnalyticsAnswer.class}, new MiruPartitionResponse<>(AnalyticsAnswer.EMPTY_RESULTS, null));
        } catch (RuntimeException e) {
            throw new MiruQueryServiceException("Failed score analytics for partition: " + partitionId.getId(), e);
        } finally {
            processMetrics.stop();
        }
    }

}
