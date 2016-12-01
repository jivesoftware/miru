package com.jivesoftware.os.miru.stream.plugins.count;

import com.google.common.base.Optional;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilterOperation;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmapsDebug;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.solution.MiruAggregateUtil;
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
import java.util.Arrays;
import java.util.List;

/**
 * @author jonathan
 */
public class DistinctCountCustomQuestion implements Question<DistinctCountQuery, DistinctCountAnswer, DistinctCountReport> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final DistinctCount distinctCount;
    private final MiruRequest<DistinctCountQuery> request;
    private final MiruRemotePartition<DistinctCountQuery, DistinctCountAnswer, DistinctCountReport> remotePartition;
    private final MiruBitmapsDebug bitmapsDebug = new MiruBitmapsDebug();
    private final MiruAggregateUtil aggregateUtil = new MiruAggregateUtil();

    public DistinctCountCustomQuestion(DistinctCount distinctCount,
        MiruRequest<DistinctCountQuery> request,
        MiruRemotePartition<DistinctCountQuery, DistinctCountAnswer, DistinctCountReport> remotePartition) {
        this.distinctCount = distinctCount;
        this.request = request;
        this.remotePartition = remotePartition;
    }

    @Override
    public <BM extends IBM, IBM> MiruPartitionResponse<DistinctCountAnswer> askLocal(MiruRequestHandle<BM, IBM, ?> handle,
        Optional<DistinctCountReport> report) throws Exception {

        StackBuffer stackBuffer = new StackBuffer();
        MiruSolutionLog solutionLog = new MiruSolutionLog(request.logLevel);
        MiruRequestContext<BM, IBM, ?> context = handle.getRequestContext();
        MiruBitmaps<BM, IBM> bitmaps = handle.getBitmaps();

        // First grab the stream filter (required)
        MiruFilter combinedFilter = request.query.streamFilter;

        // If we have a constraints filter grab that as well and AND it to the stream filter
        if (!MiruFilter.NO_FILTER.equals(request.query.constraintsFilter)) {
            combinedFilter = new MiruFilter(MiruFilterOperation.and, false, null,
                Arrays.asList(request.query.streamFilter, request.query.constraintsFilter));
        }

        // Start building up list of bitmap operations to run
        List<IBM> ands = new ArrayList<>();
        int lastId = context.getActivityIndex().lastId(stackBuffer);

        // 1) Execute the combined filter above on the given stream, add the bitmap
        BM filtered = aggregateUtil.filter("distinctCountCustom", bitmaps, context, combinedFilter, solutionLog, null, lastId, -1, -1, stackBuffer);
        ands.add(filtered);

        // 2) Add in the authz check if we have it
        if (!MiruAuthzExpression.NOT_PROVIDED.equals(request.authzExpression)) {
            ands.add(context.getAuthzIndex().getCompositeAuthz(request.authzExpression, stackBuffer));
        }

        // 3) Add in a time-range mask if we have it
        if (!MiruTimeRange.ALL_TIME.equals(request.query.timeRange)) {
            MiruTimeRange timeRange = request.query.timeRange;
            ands.add(bitmaps.buildTimeRangeMask(context.getTimeIndex(), timeRange.smallestTimestamp, timeRange.largestTimestamp, stackBuffer));
        }

        // 4) Mask out anything that hasn't made it into the activityIndex yet, or that has been removed from the index
        ands.add(bitmaps.buildIndexMask(lastId, context.getRemovalIndex(), null, stackBuffer));

        // AND it all together and return the results
        bitmapsDebug.debug(solutionLog, bitmaps, "ands", ands);
        BM answer = bitmaps.and(ands);

        return new MiruPartitionResponse<>(distinctCount.numberOfDistincts("distinctCountCustom", bitmaps, context, request, report, answer),
            solutionLog.asList());
    }

    @Override
    public MiruPartitionResponse<DistinctCountAnswer> askRemote(MiruHost host,
        MiruPartitionId partitionId,
        Optional<DistinctCountReport> report) throws MiruQueryServiceException {
        return remotePartition.askRemote(host, partitionId, request, report);
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

}
