package com.jivesoftware.os.miru.stream.plugins.catwalk;

import com.google.common.base.Optional;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.MinMaxPriorityQueue;
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
import com.jivesoftware.os.miru.plugin.cache.MiruPluginCacheProvider;
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
    private final int topNValuesPerFeature;
    private final int topNTermsPerNumerator;
    private final long maxHeapPressureInBytes;

    private final MiruBitmapsDebug bitmapsDebug = new MiruBitmapsDebug();
    private final MiruAggregateUtil aggregateUtil = new MiruAggregateUtil();

    public CatwalkQuestion(Catwalk catwalk,
        MiruRequest<CatwalkQuery> request,
        MiruRemotePartition<CatwalkQuery, CatwalkAnswer, CatwalkReport> remotePartition,
        int topNValuesPerFeature,
        int topNTermsPerNumerator,
        long maxHeapPressureInBytes) {
        this.catwalk = catwalk;
        this.request = request;
        this.remotePartition = remotePartition;
        this.topNValuesPerFeature = topNValuesPerFeature;
        this.topNTermsPerNumerator = topNTermsPerNumerator;
        this.maxHeapPressureInBytes = maxHeapPressureInBytes;
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
            return new MiruPartitionResponse<>(
                catwalk.model("catwalk", bitmaps, context, request, handle.getCoord(), report, 1, null, answerBitmap -> true, null, null, solutionLog),
                solutionLog.asList());
        }

        int lastId = context.getActivityIndex().lastId(stackBuffer);

        List<IBM> ands = new ArrayList<>();
        ands.add(bitmaps.buildIndexMask(lastId, context.getRemovalIndex(), null, stackBuffer));

        if (!MiruAuthzExpression.NOT_PROVIDED.equals(request.authzExpression)) {
            ands.add(context.getAuthzIndex().getCompositeAuthz(request.authzExpression, stackBuffer));
        }

        IBM timeRangeMask = null;
        if (!MiruTimeRange.ALL_TIME.equals(request.query.timeRange)) {
            timeRangeMask = bitmaps.buildTimeRangeMask(context.getTimeIndex(), timeRange.smallestTimestamp, timeRange.largestTimestamp, stackBuffer);
            ands.add(timeRangeMask);
        }

        Set<MiruTermId> termIds = Sets.newHashSet();
        int pivotFieldId = context.getSchema().getFieldId(request.query.gatherField);

        @SuppressWarnings("unchecked")
        Set<MiruTermId>[] numeratorTermSets = new Set[request.query.gatherFilters.length];
        for (int i = 0; i < numeratorTermSets.length; i++) {
            MiruFilter gatherFilter = request.query.gatherFilters[i];
            List<IBM> gatherAnds;
            if (MiruFilter.NO_FILTER.equals(gatherFilter)) {
                gatherAnds = ands;
            } else {
                gatherAnds = Lists.newArrayList(ands);
                gatherAnds.add(aggregateUtil.filter("catwalkGather",
                    bitmaps,
                    context,
                    gatherFilter,
                    solutionLog,
                    null,
                    lastId,
                    -1,
                    stackBuffer));
            }
            bitmapsDebug.debug(solutionLog, bitmaps, "gatherAnds", gatherAnds);
            BM eligible = bitmaps.and(gatherAnds);

            MinMaxPriorityQueue<TermIdAndCount> topNTermIds = MinMaxPriorityQueue.maximumSize(topNTermsPerNumerator).create();
            aggregateUtil.gather("catwalk", bitmaps, context, eligible, pivotFieldId, 100, false, true, solutionLog, (lastId1, termId, count) -> {
                topNTermIds.add(new TermIdAndCount(termId, count));
                return true;
            }, stackBuffer);

            numeratorTermSets[i] = Sets.newHashSet(Collections2.transform(topNTermIds, input -> input.termId));
            termIds.addAll(numeratorTermSets[i]);
        }

        CatwalkFeature[] features = request.query.features;
        IBM[] featureMasks = bitmaps.createArrayOf(features.length);

        for (int i = 0; i < features.length; i++) {
            if (!MiruFilter.NO_FILTER.equals(features[i].featureFilter)) {
                BM constrainFeature = aggregateUtil.filter("catwalkFeature",
                    bitmaps,
                    context,
                    features[i].featureFilter,
                    solutionLog,
                    null,
                    lastId,
                    -1,
                    stackBuffer);

                featureMasks[i] = constrainFeature;
            }
        }

        //TODO this duplicates StrutModelScorer behavior
        int payloadSize = 4; // this is amazing
        MiruPluginCacheProvider.TimestampedCacheKeyValues termFeaturesCache = request.query.catwalkId == null ? null
            : context.getCacheProvider().getTimestampedKeyValues("strut-features-" + request.query.catwalkId, payloadSize, false, maxHeapPressureInBytes);

        List<MiruTermId> uniqueTermIds = Lists.newArrayList(termIds);
        return new MiruPartitionResponse<>(
            catwalk.model("catwalk",
                bitmaps,
                context,
                request,
                handle.getCoord(),
                report,
                topNValuesPerFeature,
                termFeaturesCache,
                answerBitmap -> {
                    FieldMultiTermTxIndex<BM, IBM> multiTermTxIndex = new FieldMultiTermTxIndex<>("catwalk", primaryFieldIndex, pivotFieldId, -1);
                    for (List<MiruTermId> batch : Lists.partition(uniqueTermIds, 100)) {
                        BM[] answers = bitmaps.createArrayOf(batch.size());
                        int[] lastIds = new int[batch.size()];
                        multiTermTxIndex.setTermIds(batch.toArray(new MiruTermId[0]));
                        bitmaps.multiTx(multiTermTxIndex, (index, lastId1, bitmap) -> {
                            answers[index] = bitmap;
                            lastIds[index] = lastId1;
                        }, stackBuffer);
                        for (int i = 0; i < answers.length; i++) {
                            if (answers[i] != null) {
                                BM[] featureAnswers = bitmaps.createArrayOf(features.length);
                                for (int j = 0; j < features.length; j++) {
                                    featureAnswers[j] = featureMasks[j] != null ? bitmaps.and(Arrays.asList(answers[i], featureMasks[j])) : answers[i];
                                }
                                if (!answerBitmap.consume(i, pivotFieldId, batch.get(i), lastIds[i], featureAnswers)) {
                                    return false;
                                }
                            }
                        }
                    }
                    return true;
                },
                featureMasks,
                numeratorTermSets,
                solutionLog),
            solutionLog.asList());
    }

    private static class TermIdAndCount implements Comparable<TermIdAndCount> {
        public final MiruTermId termId;
        public final long count;

        public TermIdAndCount(MiruTermId termId, long count) {
            this.termId = termId;
            this.count = count;
        }

        @Override
        public int compareTo(TermIdAndCount o) {
            int c = Long.compare(o.count, count); // descending
            if (c != 0) {
                return c;
            }
            return termId.compareTo(o.termId); // for stability
        }
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
