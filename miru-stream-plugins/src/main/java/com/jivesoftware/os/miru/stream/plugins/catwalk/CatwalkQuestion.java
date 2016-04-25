package com.jivesoftware.os.miru.stream.plugins.catwalk;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
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
import com.jivesoftware.os.miru.plugin.index.FieldMultiTermTxIndex;
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
import com.jivesoftware.os.miru.stream.plugins.catwalk.CatwalkQuery.CatwalkFeature;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

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
            BM[] featureAnswers = bitmaps.createArrayOf(request.query.features.length);
            return new MiruPartitionResponse<>(
                catwalk.model("catwalk", bitmaps, context, request, handle.getCoord(), report, featureAnswers, null, null, solutionLog),
                solutionLog.asList());
        }

        int lastId = context.getActivityIndex().lastId(stackBuffer);

        List<IBM> ands = new ArrayList<>();
        ands.add(bitmaps.buildIndexMask(lastId, context.getRemovalIndex().getIndex(stackBuffer)));

        @SuppressWarnings("unchecked")
        Set<MiruTermId>[] numeratorTermSets = new Set[request.query.gatherFilters.length];
        for (int i = 0; i < numeratorTermSets.length; i++) {
            numeratorTermSets[i] = Sets.newHashSet();
        }

        if (!MiruAuthzExpression.NOT_PROVIDED.equals(request.authzExpression)) {
            ands.add(context.getAuthzIndex().getCompositeAuthz(request.authzExpression, stackBuffer));
        }

        IBM timeRangeMask = null;
        if (!MiruTimeRange.ALL_TIME.equals(request.query.timeRange)) {
            timeRangeMask = bitmaps.buildTimeRangeMask(context.getTimeIndex(), timeRange.smallestTimestamp, timeRange.largestTimestamp, stackBuffer);
            ands.add(timeRangeMask);
        }

        ands.add(bitmaps.buildIndexMask(context.getActivityIndex().lastId(stackBuffer), context.getRemovalIndex().getIndex(stackBuffer)));

        Set<MiruTermId> termIds = Sets.newHashSet();
        int pivotFieldId = context.getSchema().getFieldId(request.query.gatherField);

        for (int i = 0; i < numeratorTermSets.length; i++) {
            int gatherIndex = i;
            MiruFilter gatherFilter = request.query.gatherFilters[i];
            List<IBM> gatherAnds;
            if (MiruFilter.NO_FILTER.equals(gatherFilter)) {
                gatherAnds = ands;
            } else {
                gatherAnds = Lists.newArrayList(ands);
                gatherAnds.add(aggregateUtil.filter("catwalkGather",
                    bitmaps,
                    context.getSchema(),
                    context.getTermComposer(),
                    context.getFieldIndexProvider(),
                    gatherFilter,
                    solutionLog,
                    null,
                    lastId,
                    -1,
                    stackBuffer));
            }
            bitmapsDebug.debug(solutionLog, bitmaps, "gatherAnds", gatherAnds);
            BM eligible = bitmaps.and(gatherAnds);

            aggregateUtil.gather("catwalk", bitmaps, context, eligible, pivotFieldId, 100, false, solutionLog, (lastId1, termId) -> {
                numeratorTermSets[gatherIndex].add(termId);
                termIds.add(termId);
                return true;
            }, stackBuffer);
        }

        FieldMultiTermTxIndex<BM, IBM> multiTermTxIndex = new FieldMultiTermTxIndex<>("catwalk", primaryFieldIndex, pivotFieldId, -1);
        multiTermTxIndex.setTermIds(termIds.toArray(new MiruTermId[0]));
        BM answer = bitmaps.orMultiTx(multiTermTxIndex, stackBuffer);

        CatwalkFeature[] features = request.query.features;
        BM[] featureAnswers = bitmaps.createArrayOf(features.length);
        IBM[] featureMasks = bitmaps.createArrayOf(features.length);

        for (int i = 0; i < features.length; i++) {
            List<IBM> featureAnds = Lists.newArrayList();
            featureAnds.add(answer);
            if (timeRangeMask != null) {
                featureAnds.add(timeRangeMask);
            }
            if (MiruFilter.NO_FILTER.equals(features[i].featureFilter)) {
                featureMasks[i] = (timeRangeMask != null) ? timeRangeMask : null;
            } else {
                BM constrainFeature = aggregateUtil.filter("catwalkFeature",
                    bitmaps,
                    context.getSchema(),
                    context.getTermComposer(),
                    context.getFieldIndexProvider(),
                    features[i].featureFilter,
                    solutionLog,
                    null,
                    lastId,
                    -1,
                    stackBuffer);
                featureAnds.add(constrainFeature);

                if (timeRangeMask != null) {
                    featureMasks[i] = bitmaps.and(Arrays.asList(constrainFeature, timeRangeMask));
                } else {
                    featureMasks[i] = constrainFeature;
                }
            }
            featureAnswers[i] = bitmaps.and(featureAnds);
        }

        return new MiruPartitionResponse<>(
            catwalk.model("catwalk", bitmaps, context, request, handle.getCoord(), report, featureAnswers, featureMasks, numeratorTermSets, solutionLog),
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
