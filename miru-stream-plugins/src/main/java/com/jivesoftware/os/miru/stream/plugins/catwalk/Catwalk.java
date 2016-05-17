package com.jivesoftware.os.miru.stream.plugins.catwalk;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruTxIndex;
import com.jivesoftware.os.miru.plugin.solution.MiruAggregateUtil;
import com.jivesoftware.os.miru.plugin.solution.MiruAggregateUtil.Feature;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.plugin.solution.SimpleInvertedIndex;
import com.jivesoftware.os.miru.stream.plugins.catwalk.CatwalkQuery.CatwalkFeature;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class Catwalk {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final MiruAggregateUtil aggregateUtil = new MiruAggregateUtil();

    public interface ConsumeAnswers<BM extends IBM, IBM> {

        boolean consume(ConsumeAnswerBitmap<BM, IBM> answerBitmap) throws Exception;
    }

    public interface ConsumeAnswerBitmap<BM extends IBM, IBM> {

        boolean consume(int index, MiruTermId answerTermId, BM[] featureAnswers) throws Exception;
    }

    public <BM extends IBM, IBM> CatwalkAnswer model(String name,
        MiruBitmaps<BM, IBM> bitmaps,
        MiruRequestContext<BM, IBM, ?> requestContext,
        MiruRequest<CatwalkQuery> request,
        MiruPartitionCoord coord,
        Optional<CatwalkReport> report,
        ConsumeAnswers<BM, IBM> consumeAnswers,
        IBM[] featureMasks,
        Set<MiruTermId>[] numeratorTermSets,
        MiruSolutionLog solutionLog) throws Exception {

        StackBuffer stackBuffer = new StackBuffer();

        if (log.isDebugEnabled()) {
            log.debug("Catwalk for request={}", request);
        }
        //System.out.println("Number of matches: " + bitmaps.cardinality(answer));

        MiruFieldIndex<BM, IBM> primaryIndex = requestContext.getFieldIndexProvider().getFieldIndex(MiruFieldType.primary);

        CatwalkFeature[] features = request.query.features;
        @SuppressWarnings("unchecked")
        Map<Feature, long[]>[] featureValueSets = new Map[features.length];
        for (int i = 0; i < features.length; i++) {
            featureValueSets[i] = Maps.newHashMap();
        }
        int[][] featureFieldIds = new int[features.length][];
        for (int i = 0; i < features.length; i++) {
            String[] featureField = features[i].featureFields;
            featureFieldIds[i] = new int[featureField.length];
            for (int j = 0; j < featureField.length; j++) {
                featureFieldIds[i][j] = requestContext.getSchema().getFieldId(featureField[j]);
            }
        }

        long[] modelCounts = new long[features.length];
        aggregateUtil.gatherFeatures(name,
            bitmaps,
            requestContext,
            streamBitmaps -> {
                return consumeAnswers.consume((index, answerTermId, featureAnswers) -> {
                    for (int i = 0; i < featureAnswers.length; i++) {
                        modelCounts[i] += bitmaps.cardinality(featureAnswers[i]);
                    }
                    return streamBitmaps.stream(index, -1, answerTermId, -1, featureAnswers, 0, null);
                });
            },
            featureFieldIds,
            (streamIndex, lastId, answerTermId, answerScoredLastId, featureId, termIds) -> {
                if (featureId >= 0) {
                    long[] numerators = featureValueSets[featureId].computeIfAbsent(new Feature(termIds), key -> new long[numeratorTermSets.length]);
                    for (int i = 0; i < numeratorTermSets.length; i++) {
                        if (numeratorTermSets[i].contains(answerTermId)) {
                            numerators[i]++;
                        }
                    }
                }
                return true;
            },
            solutionLog,
            stackBuffer);

        long start = System.currentTimeMillis();

        @SuppressWarnings("unchecked")
        List<FeatureScore>[] featureScoreResults = new List[features.length];

        for (int i = 0; i < featureValueSets.length; i++) {
            Map<Feature, long[]> valueSet = featureValueSets[i];
            featureScoreResults[i] = Lists.newArrayListWithCapacity(valueSet.size());
            for (Map.Entry<Feature, long[]> entry : valueSet.entrySet()) {
                int[] fieldIds = featureFieldIds[i];
                MiruTermId[] termIds = entry.getKey().termIds;

                List<MiruTxIndex<IBM>> ands = Lists.newArrayList();
                for (int j = 0; j < fieldIds.length; j++) {
                    ands.add(primaryIndex.get(name, fieldIds[j], termIds[j]));
                }

                if (featureMasks != null && featureMasks[i] != null) {
                    ands.add(new SimpleInvertedIndex<>(featureMasks[i]));
                }

                BM bitmap = bitmaps.andTx(ands, stackBuffer);
                long[] numerators = entry.getValue();
                long denominator = bitmaps.cardinality(bitmap);
                for (int j = 0; j < numerators.length; j++) {
                    if (numerators[j] > denominator) {
                        log.warn("Catwalk computed numerators:{} index:{} denominator:{} for tenantId:{} partitionId:{} featureId:{} fieldIds:{} terms:{}",
                            Arrays.toString(numerators), j, denominator, coord.tenantId, coord.partitionId, i, Arrays.toString(fieldIds),
                            Arrays.toString(termIds));
                    }
                }
                featureScoreResults[i].add(new FeatureScore(termIds, numerators, denominator, 1));
            }
        }

        solutionLog.log(MiruSolutionLogLevel.INFO, "Gather cardinalities took {} ms", System.currentTimeMillis() - start);

        boolean resultsExhausted = request.query.timeRange.smallestTimestamp > requestContext.getTimeIndex().getLargestTimestamp();
        boolean resultsClosed = requestContext.isClosed();

        MiruTimeRange timeRange = new MiruTimeRange(
            requestContext.getTimeIndex().getSmallestTimestamp(),
            requestContext.getTimeIndex().getLargestTimestamp());

        long totalCount = requestContext.getTimeIndex().lastId();
        CatwalkAnswer result = new CatwalkAnswer(featureScoreResults, modelCounts, totalCount, timeRange, resultsExhausted, resultsClosed);
        log.debug("result={}", result);
        return result;
    }

}
