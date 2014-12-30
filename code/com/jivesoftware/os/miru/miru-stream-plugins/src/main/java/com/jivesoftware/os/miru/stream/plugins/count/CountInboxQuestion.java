package com.jivesoftware.os.miru.stream.plugins.count;

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
public class CountInboxQuestion implements Question<DistinctCountAnswer, DistinctCountReport> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final NumberOfDistincts numberOfDistincts;
    private final MiruJustInTimeBackfillerizer backfillerizer;
    private final MiruRequest<DistinctCountQuery> request;
    private final boolean unreadOnly;
    private final MiruBitmapsDebug bitmapsDebug = new MiruBitmapsDebug();
    private final MiruAggregateUtil aggregateUtil = new MiruAggregateUtil();

    public CountInboxQuestion(NumberOfDistincts numberOfDistincts,
            MiruJustInTimeBackfillerizer backfillerizer,
            MiruRequest<DistinctCountQuery> request,
            boolean unreadOnly) {

        Preconditions.checkArgument(!MiruStreamId.NULL.equals(request.query.streamId), "Inbox queries require a streamId");
        this.numberOfDistincts = numberOfDistincts;
        this.backfillerizer = backfillerizer;
        this.request = request;
        this.unreadOnly = unreadOnly;
    }

    @Override
    public <BM> MiruPartitionResponse<DistinctCountAnswer> askLocal(MiruRequestHandle<BM> handle, Optional<DistinctCountReport> report) throws Exception {
        MiruSolutionLog solutionLog = new MiruSolutionLog(request.logLevel);
        MiruRequestContext<BM> stream = handle.getRequestContext();
        MiruBitmaps<BM> bitmaps = handle.getBitmaps();

        if (handle.canBackfill()) {
            backfillerizer.backfill(bitmaps, stream, request.query.streamFilter, solutionLog, request.tenantId,
                handle.getCoord().partitionId, request.query.streamId);
        }

        List<BM> ands = new ArrayList<>();
        if (!MiruTimeRange.ALL_TIME.equals(request.query.timeRange)) {
            MiruTimeRange timeRange = request.query.timeRange;

            // Short-circuit if the time range doesn't live here
            if (!timeIndexIntersectsTimeRange(stream.getTimeIndex(), timeRange)) {
                LOG.debug("No time index intersection");
                return new MiruPartitionResponse<>(numberOfDistincts.numberOfDistincts(
                        bitmaps, stream, request, report, bitmaps.create()), solutionLog.asList());
            }
            ands.add(bitmaps.buildTimeRangeMask(stream.getTimeIndex(), timeRange.smallestTimestamp, timeRange.largestTimestamp));
        }

        Optional<BM> inbox = stream.getInboxIndex().getInbox(request.query.streamId);
        if (inbox.isPresent()) {
            ands.add(inbox.get());
        } else {
            // Short-circuit if the user doesn't have an inbox here
            LOG.debug("No user inbox");
            return new MiruPartitionResponse<>(numberOfDistincts.numberOfDistincts(bitmaps, stream, request, report, bitmaps.create()), solutionLog.asList());
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

        return new MiruPartitionResponse<>(numberOfDistincts.numberOfDistincts(bitmaps, stream, request, report, answer), solutionLog.asList());
    }

    @Override
    public MiruPartitionResponse<DistinctCountAnswer> askRemote(RequestHelper requestHelper, MiruPartitionId partitionId, Optional<DistinctCountReport> report)
            throws Exception {
        DistinctCountRemotePartitionReader reader = new DistinctCountRemotePartitionReader(requestHelper);
        if (unreadOnly) {
            return reader.countInboxStreamUnread(partitionId, request, report);
        }
        return reader.countInboxStreamAll(partitionId, request, report);
    }

    @Override
    public Optional<DistinctCountReport> createReport(Optional<DistinctCountAnswer> answer) {
        Optional<DistinctCountReport> report = Optional.absent();
        if (answer.isPresent()) {
            report = Optional.of(new DistinctCountReport(
                    answer.get().aggregateTerms,
                    answer.get().collectedDistincts));
        }
        return report;
    }

    private boolean timeIndexIntersectsTimeRange(MiruTimeIndex timeIndex, MiruTimeRange timeRange) {
        return timeRange.smallestTimestamp <= timeIndex.getLargestTimestamp() &&
                timeRange.largestTimestamp >= timeIndex.getSmallestTimestamp();
    }
}
