package com.jivesoftware.os.miru.stream.plugins.catwalk;

import com.google.common.base.Optional;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.catwalk.shared.CatwalkQuery;
import com.jivesoftware.os.miru.catwalk.shared.CatwalkQuery.CatwalkFeature;
import com.jivesoftware.os.miru.catwalk.shared.FeatureScore;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.cache.MiruPluginCacheProvider.TimestampedCacheKeyValues;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.BitmapAndLastId;
import com.jivesoftware.os.miru.plugin.index.MiruActivityIndex;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruTxIndex;
import com.jivesoftware.os.miru.plugin.solution.MiruAggregateUtil;
import com.jivesoftware.os.miru.plugin.solution.MiruAggregateUtil.Feature;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.plugin.solution.SimpleInvertedIndex;
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

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final boolean verboseLogging;

    private final MiruAggregateUtil aggregateUtil = new MiruAggregateUtil();

    public Catwalk(boolean verboseLogging) {
        this.verboseLogging = verboseLogging;
    }

    public interface ConsumeAnswers<BM extends IBM, IBM> {

        boolean consume(ConsumeAnswerBitmap<BM, IBM> answerBitmap) throws Exception;
    }

    public interface ConsumeAnswerBitmap<BM extends IBM, IBM> {

        boolean consume(int index, int answerFieldId, MiruTermId answerTermId, int answerScoredToLastId, BM[] featureAnswers) throws Exception;
    }

    public <BM extends IBM, IBM> CatwalkAnswer model(String name,
        MiruBitmaps<BM, IBM> bitmaps,
        MiruRequestContext<BM, IBM, ?> requestContext,
        MiruRequest<CatwalkQuery> request,
        MiruPartitionCoord coord,
        Optional<CatwalkReport> report,
        int topNValuesPerFeature,
        TimestampedCacheKeyValues termFeatureCache,
        ConsumeAnswers<BM, IBM> consumeAnswers,
        IBM[] featureMasks,
        Set<MiruTermId>[] numeratorTermSets,
        MiruSolutionLog solutionLog) throws Exception {

        StackBuffer stackBuffer = new StackBuffer();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Catwalk for request={}", request);
        }
        //System.out.println("Number of matches: " + bitmaps.cardinality(answer));

        MiruFieldIndex<BM, IBM> primaryIndex = requestContext.getFieldIndexProvider().getFieldIndex(MiruFieldType.primary);
        MiruActivityIndex activityIndex = requestContext.getActivityIndex();
        MiruSchema schema = requestContext.getSchema();

        CatwalkFeature[] features = request.query.features;
        @SuppressWarnings("unchecked")
        Map<Feature, FeatureBag>[] featureValueSets = new Map[features.length];
        for (int i = 0; i < features.length; i++) {
            featureValueSets[i] = Maps.newHashMap();
        }
        int[][] featureFieldIds = new int[features.length][];
        for (int i = 0; i < features.length; i++) {
            String[] featureField = features[i].featureFields;
            featureFieldIds[i] = new int[featureField.length];
            for (int j = 0; j < featureField.length; j++) {
                featureFieldIds[i][j] = schema.getFieldId(featureField[j]);
            }
        }

        long[] modelCounts = new long[features.length];
        aggregateUtil.gatherFeatures(name,
            coord,
            bitmaps,
            schema,
            activityIndex::getAll,
            termFeatureCache,
            streamBitmaps -> {
                return consumeAnswers.consume((index, answerFieldId, answerTermId, answerScoredToLastId, featureAnswers) -> {
                    for (int i = 0; i < featureAnswers.length; i++) {
                        modelCounts[i] += bitmaps.cardinality(featureAnswers[i]);
                    }
                    return streamBitmaps.stream(index, -1, answerFieldId, answerTermId, answerScoredToLastId, featureAnswers);
                });
            },
            featureFieldIds,
            topNValuesPerFeature,
            (streamIndex, lastId, answerFieldId, answerTermId, answerScoredLastId, featureId, termIds, count) -> {
                if (featureId >= 0) {
                    FeatureBag featureBag = featureValueSets[featureId].computeIfAbsent(new Feature(featureId, termIds),
                        key -> new FeatureBag(numeratorTermSets.length));
                    for (int i = 0; i < numeratorTermSets.length; i++) {
                        if (numeratorTermSets[i].contains(answerTermId)) {
                            featureBag.numerators[i] += count;
                            featureBag.answers.add(answerTermId, count);
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
        int valid = 0;
        int invalid = 0;

        for (int i = 0; i < featureValueSets.length; i++) {
            Map<Feature, FeatureBag> valueSet = featureValueSets[i];
            featureScoreResults[i] = Lists.newArrayListWithCapacity(valueSet.size());
            for (Map.Entry<Feature, FeatureBag> entry : valueSet.entrySet()) {
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
                FeatureBag featureBag = entry.getValue();
                long denominator = bitmaps.cardinality(bitmap);
                for (int j = 0; j < featureBag.numerators.length; j++) {
                    if (featureBag.numerators[j] > denominator) {
                        invalid++;
                        LOG.warn("Catwalk computed numerators[{}]:{} denominator:{}" +
                                " for name:{} tenantId:{} partitionId:{} catwalkId:{} featureId:{} fieldIds:{} terms:{}" +
                                " from answers:{}",
                            j, Arrays.toString(featureBag.numerators), denominator, name, coord.tenantId, coord.partitionId,
                            request.query.catwalkId, i, Arrays.toString(fieldIds),
                            Arrays.toString(termIds), featureBag.answers);
                        if (verboseLogging) {
                            List<IBM> verboseAnds = Lists.newArrayList();
                            for (int k = 0; k < fieldIds.length; k++) {
                                BitmapAndLastId<BM> container = new BitmapAndLastId<>();
                                primaryIndex.get(name, fieldIds[k], termIds[k]).getIndex(container, stackBuffer);
                                BM termBitmap = container.getBitmap();
                                verboseAnds.add(termBitmap);
                                LOG.info("Used field:{} term:{} cardinality:{} bitmap:{}",
                                    fieldIds[k], termIds[k], bitmaps.cardinality(termBitmap), termBitmap);
                            }
                            if (featureMasks != null && featureMasks[i] != null) {
                                verboseAnds.add(featureMasks[i]);
                                LOG.info("Masked cardinality:{} bitmap:{}", bitmaps.cardinality(featureMasks[i]), featureMasks[i]);
                            }
                            BM verboseResult = bitmaps.and(verboseAnds);
                            LOG.info("Combined result cardinality:{} bitmap:{}", bitmaps.cardinality(verboseResult), verboseResult);
                        }
                    } else {
                        valid++;
                    }
                }
                featureScoreResults[i].add(new FeatureScore(termIds, featureBag.numerators, denominator, 1));
            }
        }

        solutionLog.log(MiruSolutionLogLevel.INFO, "Gather cardinalities took {} ms", System.currentTimeMillis() - start);
        if (verboseLogging) {
            LOG.info("Catwalk gathered valid:{} invalid:{} for name:{} tenantId:{} partitionId:{} catwalkId:{}",
                valid, invalid, name, coord.tenantId, coord.partitionId, request.query.catwalkId);
        }

        boolean resultsExhausted = request.query.timeRange.smallestTimestamp > requestContext.getTimeIndex().getLargestTimestamp();
        boolean resultsClosed = requestContext.isClosed();

        MiruTimeRange timeRange = new MiruTimeRange(
            requestContext.getTimeIndex().getSmallestTimestamp(),
            requestContext.getTimeIndex().getLargestTimestamp());

        long totalCount = requestContext.getTimeIndex().lastId();
        CatwalkAnswer result = new CatwalkAnswer(featureScoreResults, modelCounts, totalCount, timeRange, resultsExhausted, resultsClosed, false);
        LOG.debug("result={}", result);
        return result;
    }

    private static class FeatureBag {
        private final long[] numerators;
        private final Multiset<MiruTermId> answers;

        public FeatureBag(int count) {
            this.numerators = new long[count];
            this.answers = HashMultiset.create();
        }
    }

}
