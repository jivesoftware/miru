package com.jivesoftware.os.miru.stream.plugins.strut;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.MinMaxPriorityQueue;
import com.jivesoftware.os.filer.io.api.KeyedFilerStore;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.filer.io.map.MapContext;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruValue;
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
import com.jivesoftware.os.miru.stream.plugins.strut.Strut.Scored;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
            StrutAnswer answer = strut.composeAnswer(context, request, Collections.emptyList(), report.isPresent() ? report.get().threshold : 0f);
            return new MiruPartitionResponse<>(answer, solutionLog.asList());
        }

        int activityIndexLastId = context.getActivityIndex().lastId(stackBuffer);

        List<IBM> ands = new ArrayList<>();
        ands.add(bitmaps.buildIndexMask(activityIndexLastId, context.getRemovalIndex().getIndex(stackBuffer)));
        MiruSchema schema = context.getSchema();
        MiruTermComposer termComposer = context.getTermComposer();

        if (!MiruFilter.NO_FILTER.equals(request.query.constraintFilter)) {
            BM constrained = aggregateUtil.filter("strutGather",
                bitmaps, schema, termComposer,
                context.getFieldIndexProvider(),
                request.query.constraintFilter,
                solutionLog,
                null,
                activityIndexLastId,
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

        ands.add(bitmaps.buildIndexMask(activityIndexLastId, context.getRemovalIndex().getIndex(stackBuffer)));

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
                activityIndexLastId,
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
        float[] thresholds = report.isPresent() ? new float[]{report.get().threshold} : new float[]{0.5f, 0.2f, 0.08f, 0f};
        @SuppressWarnings("unchecked")
        MinMaxPriorityQueue<Scored>[] scored = new MinMaxPriorityQueue[thresholds.length];

        for (int i = 0; i < thresholds.length; i++) {
            scored[i] = MinMaxPriorityQueue
                .expectedSize(request.query.desiredNumberOfResults)
                .maximumSize(request.query.desiredNumberOfResults)
                .create();
        }

        KeyedFilerStore<Integer, MapContext> cacheStore = handle.getRequestContext().getCacheProvider().get("strut");

        StrutModelScorer modelScorer = new StrutModelScorer(); // Ahh
        List<Scored> updates = Lists.newArrayList();
        strut.yourStuff("strut",
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

                
                int batchSize = 100; //TODO config batch size
                BM[] answers = bitmaps.createArrayOf(batchSize);
                int[] lastIds = new int[batchSize];
                MiruTermId[] nullableMiruTermIds = new MiruTermId[batchSize];
                MiruTermId[] miruTermIds = new MiruTermId[batchSize];
                done:
                for (List<MiruTermId> batch : Lists.partition(termIds, answers.length)) {

                    Arrays.fill(miruTermIds, null);
                    batch.toArray(miruTermIds);
                    System.arraycopy(miruTermIds, 0, nullableMiruTermIds, 0, batchSize);
                    Arrays.fill(lastIds, -1);

                    primaryIndex.multiGetLastIds("strut", pivotFieldId, nullableMiruTermIds, lastIds, stackBuffer);

                    boolean[] missed = {false};
                    modelScorer.score(request.tenantId,
                        request.query.catwalkId,
                        request.query.modelId,
                        miruTermIds,
                        cacheStore,
                        (int termIndex, float score, int lastId) -> {
                            if (!Float.isNaN(score) && lastId >= lastIds[termIndex]) {
                                miruTermIds[termIndex] = null;
                                Scored s = new Scored(miruTermIds[termIndex], lastIds[termIndex], score, -1, null);
                                for (int j = 0; j < thresholds.length; j++) {
                                    scored[j].add(s);
                                }
                            } else {
                                missed[0] = true;
                            }
                            return true;
                        },
                        stackBuffer);

                    if (missed[0]) {
                        Arrays.fill(answers, null);
                        bitmaps.multiTx(
                            (tx, stackBuffer1) -> primaryIndex.multiTxIndex("strut", pivotFieldId, miruTermIds, -1, stackBuffer1, tx),
                            (index, bitmap) -> {
                                if (constrainFeature != null) {
                                    bitmaps.inPlaceAnd(bitmap, constrainFeature);
                                }
                                answers[index] = bitmap;
                            },
                            stackBuffer);

                        for (int i = 0; i < batch.size(); i++) {
                            if (answers[i] != null && !streamBitmaps.stream(batch.get(i), lastIds[i], answers[i])) {
                                break done;
                            }
                        }
                    }
                }

                solutionLog.log(MiruSolutionLogLevel.INFO, "Strut scores took {} ms",
                    termIds.size(), System.currentTimeMillis() - start);
                return true;
            },
            thresholds,
            (thresholdIndex, hotness) -> {
                scored[thresholdIndex].add(hotness);
                // TODO persit in local cache
                updates.add(hotness);
                return true;
            },
            solutionLog);

        if (!updates.isEmpty()) {
            // Async??
            modelScorer.commit(request.tenantId, request.query.catwalkId, request.query.modelId, cacheStore, updates, stackBuffer);
        }

        MiruFieldDefinition pivotFieldDefinition = schema.getFieldDefinition(pivotFieldId);
        List<HotOrNot> hotOrNots = new ArrayList<>(request.query.desiredNumberOfResults);
        float scoredThreshold = 0f;
        for (int i = 0; i < scored.length; i++) {
            if (i == scored.length - 1 || scored[i].size() == request.query.desiredNumberOfResults) {
                for (Scored s : scored[i]) {
                    hotOrNots.add(new HotOrNot(new MiruValue(termComposer.decompose(schema, pivotFieldDefinition, stackBuffer, s.term)),
                        s.score, s.termCount, s.features));
                }
                solutionLog.log(MiruSolutionLogLevel.INFO, "Strut found {} terms at threshold {}", hotOrNots.size(), thresholds[i]);
                scoredThreshold = thresholds[i];
                break;
            }
        }

        return new MiruPartitionResponse<>(strut.composeAnswer(context, request, hotOrNots, scoredThreshold), solutionLog.asList());
    }

    @Override
    public MiruPartitionResponse<StrutAnswer> askRemote(MiruHost host,
        MiruPartitionId partitionId,
        Optional<StrutReport> report) throws MiruQueryServiceException {
        return remotePartition.askRemote(host, partitionId, request, report);
    }

    @Override
    public Optional<StrutReport> createReport(Optional<StrutAnswer> answer) {
        return answer.transform(input -> new StrutReport(input.threshold));
    }
}
