package com.jivesoftware.os.miru.stumptown.plugins;

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

import static com.jivesoftware.os.miru.stumptown.plugins.StumptownConstants.CUSTOM_QUERY_ENDPOINT;
import static com.jivesoftware.os.miru.stumptown.plugins.StumptownConstants.STUMPTOWN_PREFIX;

/**
 *
 */
public class StumptownRemotePartitionReader {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final EndPointMetrics processMetrics;
    private final RequestHelper requestHelper;

    public StumptownRemotePartitionReader(RequestHelper requestHelper) {
        this.requestHelper = requestHelper;
        this.processMetrics = new EndPointMetrics("process", LOG);
    }

    public MiruPartitionResponse<StumptownAnswer> scoreStumptowning(MiruPartitionId partitionId, MiruRequest<StumptownQuery> request,
        Optional<StumptownReport> report) throws MiruQueryServiceException {

        MiruRequestAndReport<StumptownQuery, StumptownReport> params = new MiruRequestAndReport<>(request, report.orNull());
        processMetrics.start();
        try {
            return requestHelper.executeRequest(params,
                STUMPTOWN_PREFIX + CUSTOM_QUERY_ENDPOINT + "/" + partitionId.getId(),
                MiruPartitionResponse.class, new Class[]{StumptownAnswer.class}, new MiruPartitionResponse<>(StumptownAnswer.EMPTY_RESULTS, null));
        } catch (RuntimeException e) {
            throw new MiruQueryServiceException("Failed score stumptown for partition: " + partitionId.getId(), e);
        } finally {
            processMetrics.stop();
        }
    }

}
