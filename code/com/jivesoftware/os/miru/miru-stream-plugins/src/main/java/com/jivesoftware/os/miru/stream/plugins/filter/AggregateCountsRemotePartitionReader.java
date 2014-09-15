package com.jivesoftware.os.miru.stream.plugins.filter;

import com.google.common.base.Optional;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.jive.utils.logger.EndPointMetrics;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.query.solution.MiruPartitionResponse;
import com.jivesoftware.os.miru.query.solution.MiruRequest;
import com.jivesoftware.os.miru.query.solution.MiruRequestAndReport;

import static com.jivesoftware.os.miru.stream.plugins.filter.AggregateCountsConstants.CUSTOM_QUERY_ENDPOINT;
import static com.jivesoftware.os.miru.stream.plugins.filter.AggregateCountsConstants.FILTER_PREFIX;
import static com.jivesoftware.os.miru.stream.plugins.filter.AggregateCountsConstants.INBOX_ALL_QUERY_ENDPOINT;
import static com.jivesoftware.os.miru.stream.plugins.filter.AggregateCountsConstants.INBOX_UNREAD_QUERY_ENDPOINT;

/**
 *
 */
public class AggregateCountsRemotePartitionReader {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final EndPointMetrics processMetrics;
    private final RequestHelper requestHelper;

    public AggregateCountsRemotePartitionReader(RequestHelper requestHelper) {
        this.requestHelper = requestHelper;
        this.processMetrics = new EndPointMetrics("process", LOG);
    }

    public MiruPartitionResponse<AggregateCountsAnswer> filterCustomStream(MiruPartitionId partitionId, MiruRequest<AggregateCountsQuery> request, Optional<AggregateCountsReport> report)
            throws MiruQueryServiceException {

        MiruRequestAndReport<AggregateCountsQuery, AggregateCountsReport> params = new MiruRequestAndReport<>(request, report.orNull());
        processMetrics.start();
        try {
            return requestHelper.executeRequest(params,
                    FILTER_PREFIX + CUSTOM_QUERY_ENDPOINT + "/" + partitionId.getId(),
                    MiruPartitionResponse.class, new Class[]{AggregateCountsAnswer.class},
                    new MiruPartitionResponse<>(AggregateCountsAnswer.EMPTY_RESULTS, null));
        } catch (RuntimeException e) {
            throw new MiruQueryServiceException("Failed filter custom stream for partition: " + partitionId.getId(), e);
        } finally {
            processMetrics.stop();
        }
    }

    public MiruPartitionResponse<AggregateCountsAnswer> filterInboxStreamAll(MiruPartitionId partitionId, MiruRequest<AggregateCountsQuery> request, Optional<AggregateCountsReport> report)
            throws MiruQueryServiceException {

        MiruRequestAndReport<AggregateCountsQuery, AggregateCountsReport> params = new MiruRequestAndReport<>(request, report.orNull());
        processMetrics.start();
        try {
            return requestHelper.executeRequest(params,
                    FILTER_PREFIX + INBOX_ALL_QUERY_ENDPOINT + "/" + partitionId.getId(),
                    MiruPartitionResponse.class, new Class[]{AggregateCountsAnswer.class},
                    new MiruPartitionResponse<>(AggregateCountsAnswer.EMPTY_RESULTS,null));
        } catch (RuntimeException e) {
            throw new MiruQueryServiceException("Failed filter inbox all stream for partition: " + partitionId.getId(), e);
        } finally {
            processMetrics.stop();
        }
    }

    public MiruPartitionResponse<AggregateCountsAnswer> filterInboxStreamUnread(MiruPartitionId partitionId, MiruRequest<AggregateCountsQuery> request, Optional<AggregateCountsReport> report)
            throws MiruQueryServiceException {

        MiruRequestAndReport<AggregateCountsQuery, AggregateCountsReport> params = new MiruRequestAndReport<>(request, report.orNull());
        processMetrics.start();
        try {
            return requestHelper.executeRequest(params,
                    FILTER_PREFIX + INBOX_UNREAD_QUERY_ENDPOINT + "/" + partitionId.getId(),
                    MiruPartitionResponse.class, new Class[]{AggregateCountsAnswer.class},
                    new MiruPartitionResponse<>(AggregateCountsAnswer.EMPTY_RESULTS,null));
        } catch (RuntimeException e) {
            throw new MiruQueryServiceException("Failed filter inbox unread stream for partition: " + partitionId.getId(), e);
        } finally {
            processMetrics.stop();
        }
    }

}
