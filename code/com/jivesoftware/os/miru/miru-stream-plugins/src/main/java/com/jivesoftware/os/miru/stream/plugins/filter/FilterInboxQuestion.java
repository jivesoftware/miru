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
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmapsDebug;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.MiruJustInTimeBackfillerizer;
import com.jivesoftware.os.miru.plugin.index.MiruTimeIndex;
import com.jivesoftware.os.miru.plugin.solution.MiruAggregateUtil;
import com.jivesoftware.os.miru.plugin.solution.MiruPartitionResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruRequestHandle;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.plugin.solution.Question;
import java.util.ArrayList;
import java.util.List;

/**
 * @author jonathan
 */
public class FilterInboxQuestion implements Question<AggregateCountsAnswer, AggregateCountsReport> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final AggregateCounts aggregateCounts;
    private final MiruJustInTimeBackfillerizer backfillerizer;
    private final MiruRequest<AggregateCountsQuery> request;
    private final boolean unreadOnly;
    private final MiruBitmapsDebug bitmapsDebug = new MiruBitmapsDebug();
    private final MiruAggregateUtil aggregateUtil = new MiruAggregateUtil();

    public FilterInboxQuestion(AggregateCounts aggregateCounts,
        MiruJustInTimeBackfillerizer backfillerizer,
        MiruRequest<AggregateCountsQuery> request,
        boolean unreadOnly) {

        Preconditions.checkArgument(!MiruStreamId.NULL.equals(request.query.streamId), "Inbox queries require a streamId");
        this.aggregateCounts = aggregateCounts;
        this.backfillerizer = backfillerizer;
        this.request = request;
        this.unreadOnly = unreadOnly;
    }

    @Override
    public <BM> MiruPartitionResponse<AggregateCountsAnswer> askLocal(MiruRequestHandle<BM> handle, Optional<AggregateCountsReport> report) throws Exception {
        MiruSolutionLog solutionLog = new MiruSolutionLog(request.logLevel);
        MiruRequestContext<BM> stream = handle.getRequestContext();
        MiruBitmaps<BM> bitmaps = handle.getBitmaps();

        if (handle.canBackfill()) {
            backfillerizer.backfill(bitmaps, stream, request.query.streamFilter, solutionLog, request.tenantId,
                handle.getCoord().partitionId, request.query.streamId);
        }

        List<BM> ands = new ArrayList<>();
        List<BM> counterAnds = new ArrayList<>();

        if (!MiruTimeRange.ALL_TIME.equals(request.query.answerTimeRange)) {
            MiruTimeRange timeRange = request.query.answerTimeRange;

            // Short-circuit if the time range doesn't live here
            if (!timeIndexIntersectsTimeRange(stream.getTimeIndex(), timeRange)) {
                LOG.debug("No answer time index intersection");
                return new MiruPartitionResponse<>(
                    aggregateCounts.getAggregateCounts(bitmaps, stream, request, report, bitmaps.create(), Optional.of(bitmaps.create())),
                    solutionLog.asList());
            }
            ands.add(bitmaps.buildTimeRangeMask(stream.getTimeIndex(), timeRange.smallestTimestamp, timeRange.largestTimestamp));
        }
        if (!MiruTimeRange.ALL_TIME.equals(request.query.countTimeRange)) {
            MiruTimeRange timeRange = request.query.countTimeRange;

            // Short-circuit if the time range doesn't live here
            if (!timeIndexIntersectsTimeRange(stream.getTimeIndex(), timeRange)) {
                LOG.debug("No count time index intersection");
                return new MiruPartitionResponse<>(
                    aggregateCounts.getAggregateCounts(bitmaps, stream, request, report, bitmaps.create(), Optional.of(bitmaps.create())),
                    solutionLog.asList());
            }
            counterAnds.add(bitmaps.buildTimeRangeMask(
                stream.getTimeIndex(), request.query.countTimeRange.smallestTimestamp, request.query.countTimeRange.largestTimestamp));
        }

        Optional<BM> inbox = stream.getInboxIndex().getInbox(request.query.streamId);
        if (inbox.isPresent()) {
            ands.add(inbox.get());
        } else {
            // Short-circuit if the user doesn't have an inbox here
            LOG.debug("No user inbox");
            return new MiruPartitionResponse<>(
                aggregateCounts.getAggregateCounts(bitmaps, stream, request, report, bitmaps.create(), Optional.of(bitmaps.create())),
                solutionLog.asList());
        }

        if (!MiruFilter.NO_FILTER.equals(request.query.constraintsFilter)) {
            BM filtered = bitmaps.create();
            aggregateUtil.filter(bitmaps, stream.getSchema(), stream.getFieldIndexProvider(), request.query.constraintsFilter, solutionLog, filtered, -1);
            ands.add(filtered);
        }

        if (!MiruAuthzExpression.NOT_PROVIDED.equals(request.authzExpression)) {
            ands.add(stream.getAuthzIndex().getCompositeAuthz(request.authzExpression));
        }

        if (unreadOnly) {
            Optional<BM> unreadIndex = stream.getUnreadTrackingIndex().getUnread(request.query.streamId);
            if (unreadIndex.isPresent()) {
                ands.add(unreadIndex.get());
            }
        }
        ands.add(bitmaps.buildIndexMask(stream.getActivityIndex().lastId(), stream.getRemovalIndex().getIndex()));

        BM answer = bitmaps.create();
        bitmapsDebug.debug(solutionLog, bitmaps, "ands", ands);
        bitmaps.and(answer, ands);

        counterAnds.add(answer);
        if (!unreadOnly) {
            // if unreadOnly is true, the read-tracking index would already be applied to the answer
            Optional<BM> unreadIndex = stream.getUnreadTrackingIndex().getUnread(request.query.streamId);
            if (unreadIndex.isPresent()) {
                counterAnds.add(unreadIndex.get());
            }
        }
        BM counter = bitmaps.create();
        bitmapsDebug.debug(solutionLog, bitmaps, "counterAnds", ands);
        bitmaps.and(counter, counterAnds);

        return new MiruPartitionResponse<>(
            aggregateCounts.getAggregateCounts(bitmaps, stream, request, report, answer, Optional.of(counter)),
            solutionLog.asList());
    }

    @Override
    public MiruPartitionResponse<AggregateCountsAnswer> askRemote(RequestHelper requestHelper,
        MiruPartitionId partitionId,
        Optional<AggregateCountsReport> report)
        throws Exception {
        AggregateCountsRemotePartitionReader reader = new AggregateCountsRemotePartitionReader(requestHelper);
        if (unreadOnly) {
            return reader.filterInboxStreamUnread(partitionId, request, report);
        }
        return reader.filterInboxStreamAll(partitionId, request, report);
    }

    @Override
    public Optional<AggregateCountsReport> createReport(Optional<AggregateCountsAnswer> answer) {
        Optional<AggregateCountsReport> report = Optional.absent();
        if (answer.isPresent()) {
            report = Optional.of(new AggregateCountsReport(
                answer.get().aggregateTerms,
                answer.get().skippedDistincts,
                answer.get().collectedDistincts));
        }
        return report;
    }

    private boolean timeIndexIntersectsTimeRange(MiruTimeIndex timeIndex, MiruTimeRange timeRange) {
        return timeRange.smallestTimestamp <= timeIndex.getLargestTimestamp() &&
            timeRange.largestTimestamp >= timeIndex.getSmallestTimestamp();
    }

}
