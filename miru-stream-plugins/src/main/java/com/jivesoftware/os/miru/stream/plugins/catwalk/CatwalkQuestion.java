package com.jivesoftware.os.miru.stream.plugins.catwalk;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmapsDebug;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
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

/**
 * @author jonathan
 */
public class CatwalkQuestion implements Question<CatwalkQuery, CatwalkAnswer, CatwalkReport> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final Catwalk catwalk;
    private final MiruRequest<CatwalkQuery> request;
    private final MiruRemotePartition<CatwalkQuery, CatwalkAnswer, CatwalkReport> remotePartition;
    private final MiruBitmapsDebug bitmapsDebug = new MiruBitmapsDebug();
    private final MiruAggregateUtil aggregateUtil = new MiruAggregateUtil();

    public CatwalkQuestion(Catwalk catwalk,
        MiruRequest<CatwalkQuery> request,
        MiruRemotePartition<CatwalkQuery, CatwalkAnswer, CatwalkReport> remotePartition) {
        this.catwalk = catwalk;
        this.request = request;
        this.remotePartition = remotePartition;
    }

    @Override
    public <BM extends IBM, IBM> MiruPartitionResponse<CatwalkAnswer> askLocal(MiruRequestHandle<BM, IBM, ?> handle,
        Optional<CatwalkReport> report)
        throws Exception {

        StackBuffer stackBuffer = new StackBuffer();
        MiruSolutionLog solutionLog = new MiruSolutionLog(request.logLevel);
        MiruRequestContext<BM, IBM, ?> context = handle.getRequestContext();
        MiruBitmaps<BM, IBM> bitmaps = handle.getBitmaps();
        MiruFieldIndex<BM, IBM> primaryFieldIndex = context.getFieldIndexProvider().getFieldIndex(MiruFieldType.primary);

        MiruTimeRange timeRange = request.query.timeRange;
        if (!context.getTimeIndex().intersects(timeRange)) {
            solutionLog.log(MiruSolutionLogLevel.WARN, "No time index intersection. Partition {}: {} doesn't intersect with {}",
                handle.getCoord().partitionId, context.getTimeIndex(), timeRange);
            return new MiruPartitionResponse<>(catwalk.model("catwalk", bitmaps, context, request, handle.getCoord(), report, bitmaps.create(), solutionLog),
                solutionLog.asList());
        }

        int lastId = context.getActivityIndex().lastId(stackBuffer);

        List<IBM> ands = new ArrayList<>();
        ands.add(bitmaps.buildIndexMask(lastId, context.getRemovalIndex().getIndex(stackBuffer)));

        if (!MiruFilter.NO_FILTER.equals(request.query.gatherFilter)) {
            BM constrained = aggregateUtil.filter("catwalkGather",
                bitmaps,
                context.getSchema(),
                context.getTermComposer(),
                context.getFieldIndexProvider(),
                request.query.gatherFilter,
                solutionLog,
                null,
                lastId,
                -1,
                stackBuffer);
            ands.add(constrained);
        }

        if (!MiruAuthzExpression.NOT_PROVIDED.equals(request.authzExpression)) {
            ands.add(context.getAuthzIndex().getCompositeAuthz(request.authzExpression, stackBuffer));
        }

        if (!MiruTimeRange.ALL_TIME.equals(request.query.timeRange)) {
            ands.add(bitmaps.buildTimeRangeMask(context.getTimeIndex(), timeRange.smallestTimestamp, timeRange.largestTimestamp, stackBuffer));
        }

        ands.add(bitmaps.buildIndexMask(context.getActivityIndex().lastId(stackBuffer), context.getRemovalIndex().getIndex(stackBuffer)));

        bitmapsDebug.debug(solutionLog, bitmaps, "ands", ands);
        BM eligible = bitmaps.and(ands);

        int pivotFieldId = context.getSchema().getFieldId(request.query.gatherField);

        List<MiruTermId> termIds = Lists.newArrayList();
        aggregateUtil.gather("catwalk", bitmaps, context, eligible, pivotFieldId, 100, solutionLog, termId -> {
            termIds.add(termId);
            return true;
        }, stackBuffer);

        BM answer;
        if (bitmaps.supportsInPlace()) {
            BM or = bitmaps.create();
            bitmaps.multiTx(
                (tx, stackBuffer1) -> primaryFieldIndex.multiTxIndex("catwalk", pivotFieldId, termIds.toArray(new MiruTermId[0]), -1, stackBuffer1, tx),
                (index, bitmap) -> bitmaps.inPlaceOr(or, bitmap),
                stackBuffer);
            answer = or;
        } else {
            List<IBM> ors = Lists.newArrayList();
            bitmaps.multiTx(
                (tx, stackBuffer1) -> primaryFieldIndex.multiTxIndex("catwalk", pivotFieldId, termIds.toArray(new MiruTermId[0]), -1, stackBuffer1, tx),
                (index, bitmap) -> ors.add(bitmap),
                stackBuffer);
            answer = bitmaps.or(ors);
        }

        if (!MiruFilter.NO_FILTER.equals(request.query.featureFilter)) {
            BM constrainFeature = aggregateUtil.filter("catwalkFeature",
                bitmaps,
                context.getSchema(),
                context.getTermComposer(),
                context.getFieldIndexProvider(),
                request.query.featureFilter,
                solutionLog,
                null,
                lastId,
                -1,
                stackBuffer);
            if (bitmaps.supportsInPlace()) {
                bitmaps.inPlaceAnd(answer, constrainFeature);
            } else {
                answer = bitmaps.and(Arrays.asList(answer, constrainFeature));
            }
        }

        return new MiruPartitionResponse<>(catwalk.model("catwalk", bitmaps, context, request, handle.getCoord(), report, answer, solutionLog),
            solutionLog.asList());
    }

    @Override
    public MiruPartitionResponse<CatwalkAnswer> askRemote(MiruHost host,
        MiruPartitionId partitionId,
        Optional<CatwalkReport> report) throws MiruQueryServiceException {
        return remotePartition.askRemote(host, partitionId, request, report);
    }

    @Override
    public Optional<CatwalkReport> createReport(Optional<CatwalkAnswer> answer) {
        return Optional.absent();
    }
}
