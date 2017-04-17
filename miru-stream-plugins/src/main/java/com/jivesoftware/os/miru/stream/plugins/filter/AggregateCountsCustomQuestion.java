package com.jivesoftware.os.miru.stream.plugins.filter;

import com.google.common.base.Optional;
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
import com.jivesoftware.os.miru.plugin.solution.MiruAggregateUtil;
import com.jivesoftware.os.miru.plugin.solution.MiruPartitionResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruRemotePartition;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruRequestHandle;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.plugin.solution.Question;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author jonathan
 */
public class AggregateCountsCustomQuestion implements Question<AggregateCountsQuery, AggregateCountsAnswer, AggregateCountsReport> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final AggregateCounts aggregateCounts;
    private final MiruJustInTimeBackfillerizer backfillerizer;
    private final MiruRequest<AggregateCountsQuery> request;
    private final MiruRemotePartition<AggregateCountsQuery, AggregateCountsAnswer, AggregateCountsReport> remotePartition;
    private final Set<MiruStreamId> verboseStreamIds;

    private final MiruBitmapsDebug bitmapsDebug = new MiruBitmapsDebug();
    private final MiruAggregateUtil aggregateUtil = new MiruAggregateUtil();

    public AggregateCountsCustomQuestion(AggregateCounts aggregateCounts,
        MiruJustInTimeBackfillerizer backfillerizer,
        MiruRequest<AggregateCountsQuery> request,
        MiruRemotePartition<AggregateCountsQuery, AggregateCountsAnswer, AggregateCountsReport> remotePartition, Set<MiruStreamId> verboseStreamIds) {
        this.aggregateCounts = aggregateCounts;
        this.backfillerizer = backfillerizer;
        this.request = request;
        this.remotePartition = remotePartition;
        this.verboseStreamIds = verboseStreamIds;
    }

    @Override
    public <BM extends IBM, IBM> MiruPartitionResponse<AggregateCountsAnswer> askLocal(MiruRequestHandle<BM, IBM, ?> handle,
        Optional<AggregateCountsReport> report)
        throws Exception {

        StackBuffer stackBuffer = new StackBuffer();

        MiruSolutionLog solutionLog = new MiruSolutionLog(request.logLevel);
        MiruRequestContext<BM, IBM, ?> context = handle.getRequestContext();
        MiruBitmaps<BM, IBM> bitmaps = handle.getBitmaps();

        MiruStreamId streamId = request.query.streamId;
        boolean verbose = verboseStreamIds != null && streamId != null && !MiruStreamId.NULL.equals(streamId) && verboseStreamIds.contains(streamId);

        MiruTimeRange answerTimeRange = request.query.answerTimeRange;
        if (!context.getTimeIndex().intersects(answerTimeRange)) {
            solutionLog.log(MiruSolutionLogLevel.WARN, "No time index intersection. Partition {}: {} doesn't intersect with {}",
                handle.getCoord().partitionId, context.getTimeIndex(), answerTimeRange);
            return new MiruPartitionResponse<>(aggregateCounts.getAggregateCounts("aggregateCountsCustom", solutionLog,
                bitmaps, context, request, handle.getCoord(), report, bitmaps.create(), Optional.absent(), verbose), solutionLog.asList());
        }

        List<IBM> ands = new ArrayList<>();
        int lastId = context.getActivityIndex().lastId(stackBuffer);

        BM filtered = aggregateUtil.filter("aggregateCountsCustom", bitmaps,
            context,
            request.query.streamFilter,
            solutionLog,
            null,
            lastId,
            -1,
            -1,
            stackBuffer);
        ands.add(filtered);

        ands.add(bitmaps.buildIndexMask(lastId, context.getRemovalIndex(), null, stackBuffer));

        if (streamId != null
            && !MiruStreamId.NULL.equals(streamId)
            && (request.query.includeUnreadState || request.query.unreadOnly)) {
            if (request.query.suppressUnreadFilter != null && handle.canBackfill()) {
                backfillerizer.backfillUnread(bitmaps,
                    context,
                    solutionLog,
                    request.tenantId,
                    handle.getCoord().partitionId,
                    streamId,
                    request.query.suppressUnreadFilter);
            }

            if (request.query.unreadOnly) {
                BitmapAndLastId<BM> container = new BitmapAndLastId<>();
                context.getUnreadTrackingIndex().getUnread(streamId).getIndex(container, stackBuffer);
                if (container.isSet()) {
                    ands.add(container.getBitmap());
                } else {
                    // Short-circuit if the user doesn't have any unread
                    LOG.debug("No user unread");
                    return new MiruPartitionResponse<>(
                        aggregateCounts.getAggregateCounts("aggregateCountsCustom", solutionLog, bitmaps, context, request, handle.getCoord(), report,
                            bitmaps.create(), Optional.of(bitmaps.create()), verbose),
                        solutionLog.asList());
                }
            }
        }

        if (!MiruAuthzExpression.NOT_PROVIDED.equals(request.authzExpression)) {
            ands.add(context.getAuthzIndex().getCompositeAuthz(request.authzExpression, stackBuffer));
        }

        if (!MiruTimeRange.ALL_TIME.equals(request.query.answerTimeRange)) {
            ands.add(bitmaps.buildTimeRangeMask(context.getTimeIndex(), answerTimeRange.smallestTimestamp, answerTimeRange.largestTimestamp, stackBuffer));
        }

        bitmapsDebug.debug(solutionLog, bitmaps, "ands", ands);
        BM answer = bitmaps.and(ands);

        BM counter = null;
        if (!MiruTimeRange.ALL_TIME.equals(request.query.countTimeRange)) {
            counter = bitmaps.and(Arrays.asList(answer, bitmaps.buildTimeRangeMask(
                context.getTimeIndex(), request.query.countTimeRange.smallestTimestamp, request.query.countTimeRange.largestTimestamp, stackBuffer)));
        }

        return new MiruPartitionResponse<>(aggregateCounts.getAggregateCounts("aggregateCountsCustom", solutionLog, bitmaps, context, request,
            handle.getCoord(), report, answer, Optional.fromNullable(counter), verbose), solutionLog.asList());
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
