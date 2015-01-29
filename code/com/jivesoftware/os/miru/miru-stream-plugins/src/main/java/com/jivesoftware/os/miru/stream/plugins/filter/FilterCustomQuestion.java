package com.jivesoftware.os.miru.stream.plugins.filter;

import com.google.common.base.Optional;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilterOperation;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmapsDebug;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.solution.MiruAggregateUtil;
import com.jivesoftware.os.miru.plugin.solution.MiruPartitionResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruRequestHandle;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.plugin.solution.Question;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author jonathan
 */
public class FilterCustomQuestion implements Question<AggregateCountsAnswer, AggregateCountsReport> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final AggregateCounts aggregateCounts;
    private final MiruRequest<AggregateCountsQuery> request;
    private final MiruBitmapsDebug bitmapsDebug = new MiruBitmapsDebug();
    private final MiruAggregateUtil aggregateUtil = new MiruAggregateUtil();

    public FilterCustomQuestion(AggregateCounts aggregateCounts, MiruRequest<AggregateCountsQuery> request) {
        this.aggregateCounts = aggregateCounts;
        this.request = request;
    }

    @Override
    public <BM> MiruPartitionResponse<AggregateCountsAnswer> askLocal(MiruRequestHandle<BM> handle, Optional<AggregateCountsReport> report) throws Exception {
        MiruSolutionLog solutionLog = new MiruSolutionLog(request.logLevel);
        MiruRequestContext<BM> context = handle.getRequestContext();
        MiruBitmaps<BM> bitmaps = handle.getBitmaps();

        MiruFilter combinedFilter = request.query.streamFilter;
        if (!MiruFilter.NO_FILTER.equals(request.query.constraintsFilter)) {
            combinedFilter = new MiruFilter(MiruFilterOperation.and, false, null,
                Arrays.asList(request.query.streamFilter, request.query.constraintsFilter));
        }

        List<BM> ands = new ArrayList<>();

        BM filtered = bitmaps.create();
        aggregateUtil.filter(bitmaps, context.getSchema(), context.getTermComposer(), context.getFieldIndexProvider(), combinedFilter, solutionLog, filtered,
            context.getActivityIndex().lastId(), -1);
        ands.add(filtered);

        ands.add(bitmaps.buildIndexMask(context.getActivityIndex().lastId(), context.getRemovalIndex().getIndex()));

        if (!MiruAuthzExpression.NOT_PROVIDED.equals(request.authzExpression)) {
            ands.add(context.getAuthzIndex().getCompositeAuthz(request.authzExpression));
        }

        if (!MiruTimeRange.ALL_TIME.equals(request.query.answerTimeRange)) {
            MiruTimeRange timeRange = request.query.answerTimeRange;
            ands.add(bitmaps.buildTimeRangeMask(context.getTimeIndex(), timeRange.smallestTimestamp, timeRange.largestTimestamp));
        }

        BM answer = bitmaps.create();
        bitmapsDebug.debug(solutionLog, bitmaps, "ands", ands);
        bitmaps.and(answer, ands);

        BM counter = null;
        if (!MiruTimeRange.ALL_TIME.equals(request.query.countTimeRange)) {
            counter = bitmaps.create();
            bitmaps.and(counter, Arrays.asList(answer, bitmaps.buildTimeRangeMask(
                context.getTimeIndex(), request.query.countTimeRange.smallestTimestamp, request.query.countTimeRange.largestTimestamp)));
        }

        return new MiruPartitionResponse<>(aggregateCounts.getAggregateCounts(bitmaps, context, request, report, answer, Optional.fromNullable(counter)),
            solutionLog.asList());
    }

    @Override
    public MiruPartitionResponse<AggregateCountsAnswer> askRemote(RequestHelper requestHelper,
        MiruPartitionId partitionId,
        Optional<AggregateCountsReport> report)
        throws Exception {
        return new AggregateCountsRemotePartitionReader(requestHelper).filterCustomStream(partitionId, request, report);
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
}
