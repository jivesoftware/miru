package com.jivesoftware.os.miru.stream.plugins.filter;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.plugin.backfill.MiruJustInTimeBackfillerizer;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmapsDebug;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.BitmapAndLastId;
import com.jivesoftware.os.miru.plugin.solution.MiruPartitionResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruRemotePartition;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruRequestHandle;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.plugin.solution.Question;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author jonathan
 */
public class AggregateCountsInboxQuestion implements Question<AggregateCountsQuery, AggregateCountsAnswer, AggregateCountsReport> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final AggregateCounts aggregateCounts;
    private final MiruJustInTimeBackfillerizer backfillerizer;
    private final MiruRequest<AggregateCountsQuery> request;
    private final MiruRemotePartition<AggregateCountsQuery, AggregateCountsAnswer, AggregateCountsReport> remotePartition;
    private final MiruBitmapsDebug bitmapsDebug = new MiruBitmapsDebug();

    public AggregateCountsInboxQuestion(AggregateCounts aggregateCounts,
        MiruJustInTimeBackfillerizer backfillerizer,
        MiruRequest<AggregateCountsQuery> request,
        MiruRemotePartition<AggregateCountsQuery, AggregateCountsAnswer, AggregateCountsReport> remotePartition) {

        Preconditions.checkArgument(!MiruStreamId.NULL.equals(request.query.streamId), "Inbox queries require a streamId");
        this.aggregateCounts = aggregateCounts;
        this.backfillerizer = backfillerizer;
        this.request = request;
        this.remotePartition = remotePartition;
    }

    @Override
    public <BM extends IBM, IBM> MiruPartitionResponse<AggregateCountsAnswer> askLocal(MiruRequestHandle<BM, IBM, ?> handle,
        Optional<AggregateCountsReport> report)
        throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        MiruSolutionLog solutionLog = new MiruSolutionLog(request.logLevel);
        MiruRequestContext<BM, IBM, ?> context = handle.getRequestContext();
        MiruBitmaps<BM, IBM> bitmaps = handle.getBitmaps();

        if (request.query.suppressUnreadFilter != null && handle.canBackfill()) {
            backfillerizer.backfill(bitmaps, context, request.query.streamFilter, solutionLog, request.tenantId,
                handle.getCoord().partitionId, request.query.streamId, request.query.suppressUnreadFilter);
        }

        List<IBM> ands = new ArrayList<>();
        List<IBM> counterAnds = new ArrayList<>();

        if (!context.getTimeIndex().intersects(request.query.answerTimeRange)) {
            LOG.debug("No answer time index intersection");
            return new MiruPartitionResponse<>(
                aggregateCounts.getAggregateCounts("aggregateCountsInbox", solutionLog, bitmaps, context, request, handle.getCoord(), report,
                    bitmaps.create(), Optional.absent()),
                solutionLog.asList());
        }

        if (!MiruTimeRange.ALL_TIME.equals(request.query.answerTimeRange)) {
            MiruTimeRange timeRange = request.query.answerTimeRange;

            ands.add(bitmaps.buildTimeRangeMask(context.getTimeIndex(), timeRange.smallestTimestamp, timeRange.largestTimestamp, stackBuffer));
        }
        if (!MiruTimeRange.ALL_TIME.equals(request.query.countTimeRange)) {
            counterAnds.add(bitmaps.buildTimeRangeMask(
                context.getTimeIndex(), request.query.countTimeRange.smallestTimestamp, request.query.countTimeRange.largestTimestamp, stackBuffer));
        }

        BitmapAndLastId<BM> container = new BitmapAndLastId<>();
        int lastId = context.getActivityIndex().lastId(stackBuffer);

        context.getInboxIndex().getInbox(request.query.streamId).getIndex(container, stackBuffer);
        if (container.isSet()) {
            ands.add(container.getBitmap());
        } else {
            // Short-circuit if the user doesn't have an inbox here
            LOG.debug("No user inbox");
            return new MiruPartitionResponse<>(
                aggregateCounts.getAggregateCounts("aggregateCountsInbox", solutionLog, bitmaps, context, request, handle.getCoord(), report,
                    bitmaps.create(), Optional.of(bitmaps.create())),
                solutionLog.asList());
        }

        if (request.query.unreadOnly) {
            context.getUnreadTrackingIndex().getUnread(request.query.streamId).getIndex(container, stackBuffer);
            if (container.isSet()) {
                ands.add(container.getBitmap());
            } else {
                // Short-circuit if the user doesn't have any unread
                LOG.debug("No user unread");
                return new MiruPartitionResponse<>(
                    aggregateCounts.getAggregateCounts("aggregateCountsInbox", solutionLog, bitmaps, context, request, handle.getCoord(), report,
                        bitmaps.create(), Optional.of(bitmaps.create())),
                    solutionLog.asList());
            }
        }

        if (!MiruAuthzExpression.NOT_PROVIDED.equals(request.authzExpression)) {
            ands.add(context.getAuthzIndex().getCompositeAuthz(request.authzExpression, stackBuffer));
        }

        ands.add(bitmaps.buildIndexMask(lastId, context.getRemovalIndex(), container, stackBuffer));

        bitmapsDebug.debug(solutionLog, bitmaps, "ands", ands);
        BM answer = bitmaps.and(ands);

        counterAnds.add(answer);
        if (!request.query.unreadOnly) {
            // if unreadOnly is true, the read-tracking index would already be applied to the answer
            context.getUnreadTrackingIndex().getUnread(request.query.streamId).getIndex(container, stackBuffer);
            if (container.isSet()) {
                counterAnds.add(container.getBitmap());
            }
        }
        bitmapsDebug.debug(solutionLog, bitmaps, "counterAnds", ands);
        BM counter = bitmaps.and(counterAnds);

        return new MiruPartitionResponse<>(
            aggregateCounts.getAggregateCounts("aggregateCountsInbox", solutionLog, bitmaps, context, request, handle.getCoord(), report,
                answer, Optional.of(counter)),
            solutionLog.asList());
    }

    @Override
    public MiruPartitionResponse<AggregateCountsAnswer> askRemote(MiruHost host,
        MiruPartitionId partitionId,
        Optional<AggregateCountsReport> report) throws MiruQueryServiceException {
        return remotePartition.askRemote(host, partitionId, request, report);
    }

    @Override
    public Optional<AggregateCountsReport> createReport(Optional<AggregateCountsAnswer> answer) {
        Optional<AggregateCountsReport> report = Optional.absent();
        if (answer.isPresent()) {

            AggregateCountsAnswer currentAnswer = answer.get();
            Map<String, AggregateCountsReportConstraint> constraintReport = Maps.newHashMapWithExpectedSize(currentAnswer.constraints.size());
            for (Map.Entry<String, AggregateCountsAnswerConstraint> entry : currentAnswer.constraints.entrySet()) {
                AggregateCountsAnswerConstraint value = entry.getValue();
                constraintReport.put(entry.getKey(),
                    new AggregateCountsReportConstraint(value.aggregateTerms, value.uncollectedTerms, value.skippedDistincts, value.collectedDistincts));
            }

            report = Optional.of(new AggregateCountsReport(constraintReport));
        }
        return report;
    }

}
