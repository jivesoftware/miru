package com.jivesoftware.os.miru.stream.plugins.strut;

import com.google.common.base.Optional;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmapsDebug;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.plugin.index.MiruTermComposer;
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
public class StrutQuestion implements Question<StrutQuery, StrutAnswer, StrutReport> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final Strut strut;
    private final MiruRequest<StrutQuery> request;
    private final MiruRemotePartition<StrutQuery, StrutAnswer, StrutReport> remotePartition;
    private final MiruBitmapsDebug bitmapsDebug = new MiruBitmapsDebug();
    private final MiruAggregateUtil aggregateUtil = new MiruAggregateUtil();

    public StrutQuestion(Strut strut,
        MiruRequest<StrutQuery> request,
        MiruRemotePartition<StrutQuery, StrutAnswer, StrutReport> remotePartition) {
        this.strut = strut;
        this.request = request;
        this.remotePartition = remotePartition;
    }

    @Override
    public <BM extends IBM, IBM> MiruPartitionResponse<StrutAnswer> askLocal(MiruRequestHandle<BM, IBM, ?> handle,
        Optional<StrutReport> report)
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
            StrutAnswer answer = strut.yourStuff("strut", handle.getCoord(), bitmaps, context, request, report, streamBitmaps -> true, solutionLog);
            return new MiruPartitionResponse<>(answer, solutionLog.asList());
        }

        int lastId = context.getActivityIndex().lastId(stackBuffer);

        List<IBM> ands = new ArrayList<>();
        ands.add(bitmaps.buildIndexMask(lastId, context.getRemovalIndex().getIndex(stackBuffer)));
        MiruSchema schema = context.getSchema();
        MiruTermComposer termComposer = context.getTermComposer();

        if (!MiruFilter.NO_FILTER.equals(request.query.constraintFilter)) {
            BM constrained = aggregateUtil.filter("strutGather",
                bitmaps, schema, termComposer,
                context.getFieldIndexProvider(),
                request.query.constraintFilter,
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

        bitmapsDebug.debug(solutionLog, bitmaps, "ands", ands);
        BM eligible = bitmaps.and(ands);

        int pivotFieldId = schema.getFieldId(request.query.constraintField);

        BM constrainFeature;
        if (!MiruFilter.NO_FILTER.equals(request.query.featureFilter)) {
            constrainFeature = aggregateUtil.filter("strutFeature",
                bitmaps, schema, termComposer,
                context.getFieldIndexProvider(),
                request.query.featureFilter,
                solutionLog,
                null,
                lastId,
                -1,
                stackBuffer);
        } else {
            constrainFeature = null;
        }

        StrutAnswer answer = strut.yourStuff("strut", handle.getCoord(), bitmaps, context, request, report, (streamBitmaps) -> {
            aggregateUtil.gather("strut", bitmaps, context, eligible, pivotFieldId, 100, solutionLog, (termId) -> {

                MiruInvertedIndex<BM, IBM> invertedIndex = context.getFieldIndexProvider()
                    .getFieldIndex(MiruFieldType.primary)
                    .get("strut-term", pivotFieldId, termId);

                Optional<BM> index = invertedIndex.getIndex(stackBuffer);
                if (index.isPresent()) {
                    BM scorable = index.get();
                    if (bitmaps.supportsInPlace()) {
                        bitmaps.inPlaceAnd(scorable, constrainFeature);
                    } else {
                        scorable = bitmaps.and(Arrays.asList(scorable, constrainFeature));
                    }

                    if (!streamBitmaps.stream(termId, scorable)) {
                        return false;
                    }
                }
                return true;
            }, stackBuffer);
            return true;
        }, solutionLog);
        return new MiruPartitionResponse<>(answer, solutionLog.asList());
    }

    @Override
    public MiruPartitionResponse<StrutAnswer> askRemote(MiruHost host,
        MiruPartitionId partitionId,
        Optional<StrutReport> report) throws MiruQueryServiceException {
        return remotePartition.askRemote(host, partitionId, request, report);
    }

    @Override
    public Optional<StrutReport> createReport(Optional<StrutAnswer> answer) {
        return Optional.absent();
    }
}
