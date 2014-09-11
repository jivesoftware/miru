package com.jivesoftware.os.miru.stream.plugins.count;

import com.google.common.base.Optional;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.jive.utils.logger.EndPointMetrics;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;

import static com.jivesoftware.os.miru.stream.plugins.count.DistinctCountConstants.COUNT_PREFIX;
import static com.jivesoftware.os.miru.stream.plugins.count.DistinctCountConstants.CUSTOM_QUERY_ENDPOINT;
import static com.jivesoftware.os.miru.stream.plugins.count.DistinctCountConstants.INBOX_ALL_QUERY_ENDPOINT;
import static com.jivesoftware.os.miru.stream.plugins.count.DistinctCountConstants.INBOX_UNREAD_QUERY_ENDPOINT;

/**
 *
 */
public class DistinctCountRemotePartitionReader {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final EndPointMetrics processMetrics;
    private final RequestHelper requestHelper;

    public DistinctCountRemotePartitionReader(RequestHelper requestHelper) {
        this.requestHelper = requestHelper;
        this.processMetrics = new EndPointMetrics("process", LOG);
    }

    public DistinctCountAnswer countCustomStream(MiruPartitionId partitionId, DistinctCountQuery query, Optional<DistinctCountReport> report)
            throws MiruQueryServiceException {

        DistinctCountQueryAndReport params = new DistinctCountQueryAndReport(query, report.orNull());
        processMetrics.start();
        try {
            return requestHelper.executeRequest(params,
                    COUNT_PREFIX + CUSTOM_QUERY_ENDPOINT + "/" + partitionId.getId(),
                    DistinctCountAnswer.class, DistinctCountAnswer.EMPTY_RESULTS);
        } catch (RuntimeException e) {
            throw new MiruQueryServiceException("Failed count custom stream for partition: " + partitionId.getId(), e);
        } finally {
            processMetrics.stop();
        }
    }

    public DistinctCountAnswer countInboxStreamAll(MiruPartitionId partitionId, DistinctCountQuery query, Optional<DistinctCountReport> report)
            throws MiruQueryServiceException {

        DistinctCountQueryAndReport params = new DistinctCountQueryAndReport(query, report.orNull());
        processMetrics.start();
        try {
            return requestHelper.executeRequest(params,
                    COUNT_PREFIX + INBOX_ALL_QUERY_ENDPOINT + "/" + partitionId.getId(),
                    DistinctCountAnswer.class, DistinctCountAnswer.EMPTY_RESULTS);
        } catch (RuntimeException e) {
            throw new MiruQueryServiceException("Failed count inbox all stream for partition: " + partitionId.getId(), e);
        } finally {
            processMetrics.stop();
        }
    }

    public DistinctCountAnswer countInboxStreamUnread(MiruPartitionId partitionId, DistinctCountQuery query, Optional<DistinctCountReport> report)
            throws MiruQueryServiceException {

        DistinctCountQueryAndReport params = new DistinctCountQueryAndReport(query, report.orNull());
        processMetrics.start();
        try {
            return requestHelper.executeRequest(params,
                    COUNT_PREFIX + INBOX_UNREAD_QUERY_ENDPOINT + "/" + partitionId.getId(),
                    DistinctCountAnswer.class, DistinctCountAnswer.EMPTY_RESULTS);
        } catch (RuntimeException e) {
            throw new MiruQueryServiceException("Failed count inbox unread stream for partition: " + partitionId.getId(), e);
        } finally {
            processMetrics.stop();
        }
    }

}
