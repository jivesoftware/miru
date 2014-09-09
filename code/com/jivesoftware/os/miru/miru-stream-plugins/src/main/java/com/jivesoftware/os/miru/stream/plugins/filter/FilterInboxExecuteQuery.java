package com.jivesoftware.os.miru.stream.plugins.filter;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.query.ExecuteMiruFilter;
import com.jivesoftware.os.miru.query.ExecuteQuery;
import com.jivesoftware.os.miru.query.MiruBitmaps;
import com.jivesoftware.os.miru.query.MiruBitmapsDebug;
import com.jivesoftware.os.miru.query.MiruJustInTimeBackfillerizer;
import com.jivesoftware.os.miru.query.MiruQueryHandle;
import com.jivesoftware.os.miru.query.MiruQueryStream;
import com.jivesoftware.os.miru.query.MiruTimeIndex;
import com.jivesoftware.os.miru.query.MiruTimeRange;
import java.util.ArrayList;
import java.util.List;

/**
 * @author jonathan
 */
public class FilterInboxExecuteQuery implements ExecuteQuery<AggregateCountsResult, AggregateCountsReport> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final AggregateCounts aggregateCounts;
    private final MiruJustInTimeBackfillerizer backfillerizer;
    private final AggregateCountsQuery query;
    private final boolean unreadOnly;
    private final MiruBitmapsDebug bitmapsDebug = new MiruBitmapsDebug();

    public FilterInboxExecuteQuery(AggregateCounts aggregateCounts,
            MiruJustInTimeBackfillerizer backfillerizer,
            AggregateCountsQuery query,
            boolean unreadOnly) {

        Preconditions.checkArgument(!MiruStreamId.NULL.equals(query.streamId), "Inbox queries require a streamId");
        this.aggregateCounts = aggregateCounts;
        this.backfillerizer = backfillerizer;
        this.query = query;
        this.unreadOnly = unreadOnly;
    }

    @Override
    public <BM> AggregateCountsResult executeLocal(MiruQueryHandle<BM> handle, Optional<AggregateCountsReport> report) throws Exception {
        MiruQueryStream<BM> stream = handle.getQueryStream();
        MiruBitmaps<BM> bitmaps = handle.getBitmaps();

        if (handle.canBackfill()) {
            backfillerizer.backfill(bitmaps, stream, query.streamFilter, query.tenantId, handle.getCoord().partitionId, query.streamId);
        }

        List<BM> ands = new ArrayList<>();
        List<BM> counterAnds = new ArrayList<>();

        if (!MiruTimeRange.ALL_TIME.equals(query.answerTimeRange)) {
            MiruTimeRange timeRange = query.answerTimeRange;

            // Short-circuit if the time range doesn't live here
            if (!timeIndexIntersectsTimeRange(stream.timeIndex, timeRange)) {
                LOG.debug("No answer time index intersection");
                return aggregateCounts.getAggregateCounts(bitmaps, stream, query, report, bitmaps.create(), Optional.of(bitmaps.create()));
            }
            ands.add(bitmaps.buildTimeRangeMask(stream.timeIndex, timeRange.smallestTimestamp, timeRange.largestTimestamp));
        }
        if (!MiruTimeRange.ALL_TIME.equals(query.countTimeRange)) {
            MiruTimeRange timeRange = query.countTimeRange;

            // Short-circuit if the time range doesn't live here
            if (!timeIndexIntersectsTimeRange(stream.timeIndex, timeRange)) {
                LOG.debug("No count time index intersection");
                return aggregateCounts.getAggregateCounts(bitmaps, stream, query, report, bitmaps.create(), Optional.of(bitmaps.create()));
            }
            counterAnds.add(bitmaps.buildTimeRangeMask(
                    stream.timeIndex, query.countTimeRange.smallestTimestamp, query.countTimeRange.largestTimestamp));
        }

        Optional<BM> inbox = stream.inboxIndex.getInbox(query.streamId);
        if (inbox.isPresent()) {
            ands.add(inbox.get());
        } else {
            // Short-circuit if the user doesn't have an inbox here
            LOG.debug("No user inbox");
            return aggregateCounts.getAggregateCounts(bitmaps, stream, query, report, bitmaps.create(), Optional.of(bitmaps.create()));
        }

        if (!MiruFilter.NO_FILTER.equals(query.constraintsFilter)) {
            ExecuteMiruFilter<BM> executeMiruFilter = new ExecuteMiruFilter<>(bitmaps, stream.schema, stream.fieldIndex, stream.executorService,
                    query.constraintsFilter, Optional.<BM>absent(), -1);
            ands.add(executeMiruFilter.call());
        }

        if (!MiruAuthzExpression.NOT_PROVIDED.equals(query.authzExpression)) {
            ands.add(stream.authzIndex.getCompositeAuthz(query.authzExpression));
        }

        if (unreadOnly) {
            Optional<BM> unreadIndex = stream.unreadTrackingIndex.getUnread(query.streamId);
            if (unreadIndex.isPresent()) {
                ands.add(unreadIndex.get());
            }
        }
        ands.add(bitmaps.buildIndexMask(stream.activityIndex.lastId(), Optional.of(stream.removalIndex.getIndex())));

        BM answer = bitmaps.create();
        bitmapsDebug.debug(LOG, bitmaps, "ands", ands);
        bitmaps.and(answer, ands);

        counterAnds.add(answer);
        if (!unreadOnly) {
            // if unreadOnly is true, the read-tracking index would already be applied to the answer
            Optional<BM> unreadIndex = stream.unreadTrackingIndex.getUnread(query.streamId);
            if (unreadIndex.isPresent()) {
                counterAnds.add(unreadIndex.get());
            }
        }
        BM counter = bitmaps.create();
        bitmapsDebug.debug(LOG, bitmaps, "counterAnds", ands);
        bitmaps.and(counter, counterAnds);

        return aggregateCounts.getAggregateCounts(bitmaps, stream, query, report, answer, Optional.of(counter));
    }

    @Override
    public AggregateCountsResult executeRemote(RequestHelper requestHelper, MiruPartitionId partitionId, Optional<AggregateCountsResult> lastResult)
            throws Exception {
        AggregateCountsRemotePartitionReader reader = new AggregateCountsRemotePartitionReader(requestHelper);
        if (unreadOnly) {
            return reader.filterInboxStreamUnread(partitionId, query, lastResult);
        }
        return reader.filterInboxStreamAll(partitionId, query, lastResult);
    }

    @Override
    public Optional<AggregateCountsReport> createReport(Optional<AggregateCountsResult> result) {
        Optional<AggregateCountsReport> report = Optional.absent();
        if (result.isPresent()) {
            report = Optional.of(new AggregateCountsReport(
                    result.get().skippedDistincts,
                    result.get().collectedDistincts,
                    result.get().aggregateTerms));
        }
        return report;
    }

    private boolean timeIndexIntersectsTimeRange(MiruTimeIndex timeIndex, MiruTimeRange timeRange) {
        return timeRange.smallestTimestamp <= timeIndex.getLargestTimestamp() &&
                timeRange.largestTimestamp >= timeIndex.getSmallestTimestamp();
    }

}
