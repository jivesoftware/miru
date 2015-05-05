package com.jivesoftware.os.miru.stream.plugins.filter;

import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.plugin.solution.MiruRemotePartition;
import com.jivesoftware.os.mlogger.core.EndPointMetrics;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;

import static com.jivesoftware.os.miru.stream.plugins.filter.AggregateCountsConstants.FILTER_PREFIX;
import static com.jivesoftware.os.miru.stream.plugins.filter.AggregateCountsConstants.INBOX_UNREAD_QUERY_ENDPOINT;

/**
 *
 */
public class AggregateCountsInboxUnreadRemotePartition implements MiruRemotePartition<AggregateCountsQuery, AggregateCountsAnswer, AggregateCountsReport> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private static final EndPointMetrics endPointMetrics = new EndPointMetrics("process", LOG);

    @Override
    public String getEndpoint(MiruPartitionId partitionId) {
        return FILTER_PREFIX + INBOX_UNREAD_QUERY_ENDPOINT + "/" + partitionId.getId();
    }

    @Override
    public Class<AggregateCountsAnswer> getAnswerClass() {
        return AggregateCountsAnswer.class;
    }

    @Override
    public AggregateCountsAnswer getEmptyResults() {
        return AggregateCountsAnswer.EMPTY_RESULTS;
    }

    @Override
    public EndPointMetrics getEndpointMetrics() {
        return endPointMetrics;
    }

}
