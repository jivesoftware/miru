package com.jivesoftware.os.miru.stream.plugins.strut;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmapsDebug;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
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

        MiruFieldIndex<BM, IBM> primaryIndex = context.getFieldIndexProvider().getFieldIndex(MiruFieldType.primary);

        /*StrutAnswer answer = strut.yourStuff("strut", handle.getCoord(), bitmaps, context, request, report, (streamBitmaps) -> {
            List<MiruTermId> termIds = Lists.newArrayList();
            aggregateUtil.gather("strut", bitmaps, context, eligible, pivotFieldId, 100, solutionLog, (termId) -> {
                termIds.add(termId);
                return true;
            }, stackBuffer);

            long start = System.currentTimeMillis();
            BM combined = bitmaps.create();
            bitmaps.multiTx(
                (tx, stackBuffer1) -> primaryIndex.multiTxIndex("strut", pivotFieldId, termIds.toArray(new MiruTermId[0]), -1, stackBuffer1, tx),
                (index1, bitmap) -> bitmaps.inPlaceOr(combined, bitmap),
                stackBuffer);
            if (constrainFeature != null) {
                bitmaps.inPlaceAnd(combined, constrainFeature);
            }
            solutionLog.log(MiruSolutionLogLevel.INFO, "Strut term accumulation for {} bitmaps took {} ms",
                termIds.size(), System.currentTimeMillis() - start);

            return streamBitmaps.stream(null, combined);
        }, solutionLog);*/

        StrutAnswer answer = strut.yourStuff("strut",
            handle.getCoord(),
            bitmaps,
            context,
            request,
            report,
            (streamBitmaps) -> {
                long start = System.currentTimeMillis();
                List<MiruTermId> termIds = Lists.newArrayList();
                //TODO config batch size
                aggregateUtil.gather("strut", bitmaps, context, eligible, pivotFieldId, 100, solutionLog, (termId) -> {
                    termIds.add(termId);
                    return true;
                }, stackBuffer);

                solutionLog.log(MiruSolutionLogLevel.INFO, "Strut accumulated {} terms in {} ms",
                    termIds.size(), System.currentTimeMillis() - start);
                start = System.currentTimeMillis();

                //TODO config batch size
                BM[] answers = bitmaps.createArrayOf(100);
                done:
                for (List<MiruTermId> batch : Lists.partition(termIds, answers.length)) {
                    Arrays.fill(answers, null);
                    bitmaps.multiTx(
                        (tx, stackBuffer1) -> primaryIndex.multiTxIndex("strut", pivotFieldId, batch.toArray(new MiruTermId[0]), -1, stackBuffer1, tx),
                        (index, bitmap) -> {
                            if (constrainFeature != null) {
                                bitmaps.inPlaceAnd(bitmap, constrainFeature);
                            }
                            answers[index] = bitmap;
                        },
                        stackBuffer);

                    for (int i = 0; i < batch.size(); i++) {
                        if (answers[i] != null && !streamBitmaps.stream(batch.get(i), answers[i])) {
                            break done;
                        }
                    }
                }

                solutionLog.log(MiruSolutionLogLevel.INFO, "Strut scores took {} ms",
                    termIds.size(), System.currentTimeMillis() - start);
                return true;
            },
            solutionLog);

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
