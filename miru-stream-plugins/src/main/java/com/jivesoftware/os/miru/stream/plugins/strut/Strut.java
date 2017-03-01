package com.jivesoftware.os.miru.stream.plugins.strut;

import com.google.common.collect.Lists;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.query.filter.MiruValue;
import com.jivesoftware.os.miru.catwalk.shared.HotOrNot;
import com.jivesoftware.os.miru.catwalk.shared.StrutModelScore;
import com.jivesoftware.os.miru.catwalk.shared.Scored;
import com.jivesoftware.os.miru.catwalk.shared.StrutModel;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.cache.MiruPluginCacheProvider.TimestampedCacheKeyValues;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.MiruActivityIndex;
import com.jivesoftware.os.miru.plugin.index.MiruTermComposer;
import com.jivesoftware.os.miru.plugin.solution.MiruAggregateUtil;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.catwalk.shared.CatwalkQuery;
import com.jivesoftware.os.miru.catwalk.shared.CatwalkQuery.CatwalkFeature;
import com.jivesoftware.os.miru.catwalk.shared.HotOrNot.Hotness;
import com.jivesoftware.os.miru.stream.plugins.strut.StrutQuery.Strategy;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public class Strut {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruAggregateUtil aggregateUtil = new MiruAggregateUtil();

    private final StrutModelCache cache;

    public Strut(StrutModelCache cache) {
        this.cache = cache;
    }

    public <BM extends IBM, IBM> StrutAnswer composeAnswer(MiruRequestContext<BM, IBM, ?> requestContext,
        MiruRequest<StrutQuery> request,
        List<HotOrNot> hotOrNots,
        int modelTotalPartitionCount) throws Exception {

        boolean resultsExhausted = request.query.timeRange.smallestTimestamp > requestContext.getTimeIndex().getLargestTimestamp();
        return new StrutAnswer(hotOrNots, modelTotalPartitionCount, resultsExhausted);
    }

    public interface HotStuff {

        boolean steamStream(int streamIndex, Scored scored, boolean cacheable);
    }

    public interface StrutBitmapStream<BM> {

        boolean stream(int streamIndex, int lastId, int fieldId, MiruTermId termId, int scoredToLastId, BM[] answers) throws Exception;
    }

    public interface StrutStream<BM> {

        boolean stream(StrutBitmapStream<BM> streamBitmaps) throws Exception;
    }

    public <BM extends IBM, IBM> void yourStuff(String name,
        MiruPartitionCoord coord,
        MiruBitmaps<BM, IBM> bitmaps,
        MiruRequestContext<BM, IBM, ?> requestContext,
        String catwalkId,
        String modelId,
        CatwalkQuery catwalkQuery,
        float[] featureScalars,
        Strategy featureStrategy,
        boolean includeFeatures,
        float[] numeratorScalars,
        Strategy numeratorStrategy,
        int topNValuesPerFeature,
        TimestampedCacheKeyValues termFeatureCache,
        StrutStream<BM> strutStream,
        HotStuff hotStuff,
        AtomicInteger totalPartitionCount,
        MiruSolutionLog solutionLog) throws Exception {

        long start = System.currentTimeMillis();
        StackBuffer stackBuffer = new StackBuffer();

        MiruSchema schema = requestContext.getSchema();
        MiruActivityIndex activityIndex = requestContext.getActivityIndex();

        MiruTermComposer termComposer = requestContext.getTermComposer();
        //CatwalkQuery catwalkQuery = request.query.catwalkQuery;
        CatwalkFeature[] catwalkFeatures = catwalkQuery.features;
        int numeratorsCount = catwalkQuery.gatherFilters.length;
        //float[] featureScalars = request.query.featureScalars;

        int[][] featureFieldIds = new int[catwalkFeatures.length][];
        for (int i = 0; i < catwalkFeatures.length; i++) {
            String[] featureField = catwalkFeatures[i].featureFields;
            featureFieldIds[i] = new int[featureField.length];
            for (int j = 0; j < featureField.length; j++) {
                featureFieldIds[i][j] = requestContext.getSchema().getFieldId(featureField[j]);
            }
        }

        @SuppressWarnings("unchecked")
        List<Hotness>[] features = includeFeatures ? new List[catwalkFeatures.length] : null;
        int[] featureCount = { 0 };

        StrutModel model = cache.get(coord.tenantId, catwalkId, modelId, coord.partitionId.getId(), catwalkQuery);
        if (model == null) {

            strutStream.stream((streamIndex, lastId, fieldId, termId, scoredToLastId, answers) -> {
                boolean more = true;
                if (!hotStuff.steamStream(streamIndex, new Scored(lastId, termId, scoredToLastId, 0f, new float[numeratorsCount], null, -1), false)) {
                    more = false;
                }
                return more;
            });

        } else {

            boolean[] cacheable = new boolean[] { true };
            for (int i = 0; i < catwalkFeatures.length; i++) {
                totalPartitionCount.set(Math.max(totalPartitionCount.get(), model.totalNumPartitions[i]));
                if (model.numberOfModels[i] == 0) {
                    cacheable[0] = false;
                }
            }

            float[][] scores = new float[numeratorsCount][catwalkFeatures.length];
            int[][] counts = new int[numeratorsCount][catwalkFeatures.length];
            @SuppressWarnings("unchecked")
            List<MiruTermId[]>[] featuredTermIds = new List[catwalkFeatures.length];
            aggregateUtil.gatherFeatures(name,
                coord,
                bitmaps,
                schema,
                activityIndex::getAll,
                termFeatureCache,
                streamBitmaps -> strutStream.stream(streamBitmaps::stream),
                featureFieldIds,
                topNValuesPerFeature,
                (streamIndex, lastId, answerFieldId, answerTermId, answerScoredLastId, featureId, termIds, count) -> {
                    if (featureId == -1) {
                        boolean stopped = false;
                        List<Hotness>[] scoredFeatures = null;
                        if (includeFeatures) {
                            scoredFeatures = new List[features.length];
                            System.arraycopy(features, 0, scoredFeatures, 0, features.length);
                        }
                        float[] termScores = new float[numeratorsCount];
                        for (int i = 0; i < termScores.length; i++) {
                            termScores[i] = finalizeScore(scores[i], counts[i], featureStrategy);
                        }
                        float scaledScore = Strut.scaleScore(termScores, numeratorScalars, numeratorStrategy);
                        Scored scored = new Scored(lastId, answerTermId, answerScoredLastId, scaledScore, termScores, scoredFeatures, count);
                        if (!hotStuff.steamStream(streamIndex, scored, cacheable[0])) {
                            stopped = true;
                        }

                        for (int i = 0; i < numeratorsCount; i++) {
                            Arrays.fill(scores[i], 0.0f);
                            Arrays.fill(counts[i], 0);
                        }
                        Arrays.fill(featuredTermIds, null);

                        if (includeFeatures) {
                            Arrays.fill(features, null);
                        }
                        if (stopped) {
                            return false;
                        }

                    } else {
                        featureCount[0]++;
                        StrutModelScore modelScore = model.score(featureId, termIds);
                        if (modelScore != null) {
                            float[] s = new float[modelScore.numerators.length];
                            for (int i = 0; i < s.length; i++) {
                                s[i] = (float) modelScore.numerators[i] / modelScore.denominator;
                                if (s[i] > 1.0f) {
                                    LOG.warn("Encountered score {} > 1.0 for answerTermId:{} numerator[{}]:{} denominator:{} featureId:{} termIds:{}",
                                        s, answerTermId, i, modelScore.numerators[i], modelScore.denominator, featureId, Arrays.toString(termIds));
                                    s[i] = 1.0f;
                                } else if (Float.isNaN(s[i])) {
                                    LOG.warn("Encountered score NaN for answerTermId:{} numerator[{}]:{} denominator:{} featureId:{} termIds:{}",
                                        answerTermId, i, modelScore.numerators[i], modelScore.denominator, featureId, Arrays.toString(termIds));
                                    s[i] = 0f;
                                }
                                scores[i][featureId] = score(scores[i][featureId], s[i], featureScalars[featureId], featureStrategy);
                                counts[i][featureId]++;
                            }

                            if (featuredTermIds[featureId] == null) {
                                featuredTermIds[featureId] = Lists.newArrayList();
                            }
                            featuredTermIds[featureId].add(termIds);

                            if (includeFeatures) {
                                if (features[featureId] == null) {
                                    features[featureId] = Lists.newArrayList();
                                }
                                MiruValue[] values = new MiruValue[termIds.length];
                                for (int j = 0; j < termIds.length; j++) {
                                    values[j] = new MiruValue(termComposer.decompose(schema,
                                        schema.getFieldDefinition(featureFieldIds[featureId][j]), stackBuffer, termIds[j]));
                                }
                                features[featureId].add(new Hotness(values, scaleScore(s, numeratorScalars, numeratorStrategy), s));
                            }
                        }
                    }
                    return true;
                },
                solutionLog,
                stackBuffer);
        }

        LOG.info("Strut scored {} features in {} ms for {}", featureCount[0], System.currentTimeMillis() - start, coord);
        solutionLog.log(MiruSolutionLogLevel.INFO, "Strut scored {} features in {} ms", featureCount[0], System.currentTimeMillis() - start);

    }

    static float score(float current, float update, float scalar, Strategy strategy) {
        if (scalar > 0f) {
            if (strategy == Strategy.UNIT_WEIGHTED || strategy == Strategy.REGRESSION_WEIGHTED || strategy == Strategy.MAX) {
                return current > 0f ? Math.max(current, update * scalar) : update * scalar;
            } else {
                throw new UnsupportedOperationException("Strategy not supported: " + strategy);
            }
        } else {
            return current;
        }
    }

    static float finalizeScore(float[] scores, int[] counts, Strategy strategy) {
        if (strategy == Strategy.UNIT_WEIGHTED) {
            float sum = 0;
            int count = 0;
            for (int i = 0; i < scores.length; i++) {
                if (scores[i] > 0f && counts[i] > 0) {
                    sum += scores[i];
                    count++;
                }
            }
            return count == 0 ? 0.0f : sum / scores.length;
        } else if (strategy == Strategy.REGRESSION_WEIGHTED) {
            float sum = 0;
            for (int i = 0; i < scores.length; i++) {
                if (scores[i] > 0f && counts[i] > 0) {
                    sum += scores[i];
                }
            }
            return sum;
        } else if (strategy == Strategy.MAX) {
            float max = 0;
            for (int i = 0; i < scores.length; i++) {
                if (scores[i] > 0f && counts[i] > 0) {
                    max = Math.max(max, scores[i]);
                }
            }
            return max;
        } else {
            throw new UnsupportedOperationException("Strategy not supported: " + strategy);
        }
    }

    static float scaleScore(float[] scores, float[] scalars, Strategy strategy) {
        float[] scaled = new float[scores.length];
        int[] scaledCounts = new int[scores.length];
        for (int i = 0; i < scaled.length; i++) {
            scaled[i] = scores[i] * scalars[i];
            scaledCounts[i] = (!Float.isNaN(scaled[i]) && scaled[i] > 0f) ? 1 : 0;
        }
        return finalizeScore(scaled, scaledCounts, strategy);
    }

    /*public static void main(String[] args) {
        float totalActivities = 3_000_000f;
        float viewedActivities = 10_000f;

        float[] viewedFeatures = { 7f, 3f, 8f };
        float[] nonViewedFeatures = { 3f, 12f, 12f };
        // 7/10, 3/15, 8/20

        float pViewed1 = (7f / 10_000f) * (3f / 10_000f) * (8f / 10_000f) * (10_000f / 3_000_000f);
        float pNonViewed1 = (3f / 2_990_000f) * (12f / 2_990_000f) * (12f / 2_990_000f) * (2_990_000f / 3_000_000f);
        float p1 = (10f / 3_000_000f) * (15f / 3_000_000f) * (20f / 3_000_000f);

        float pViewed2 = (5f / 15_000f) * (6f / 15_000f) * (10f / 15_000f) * (15_000f / 3_000_000f);
        float pNonViewed2 = (8f / 2_985_000f) * (2f / 2_985_000f) * (2f / 2_985_000f) * (2_985_000f / 3_000_000f);

        //System.out.println(pViewed1);
        //System.out.println(pNonViewed1);
        System.out.println("pV1: " + pViewed1);
        System.out.println("pNV1: " + pNonViewed1);
        System.out.println("p1: " + p1);
        System.out.println("pV1/p1: " + (pViewed1 / p1));
        System.out.println("pNV1/p1: " + (pNonViewed1 / p1));
        System.out.println("---");
        System.out.println(pViewed2 / pNonViewed2);
        System.out.println((pViewed1 * pViewed2) / (pNonViewed1 * pNonViewed2));
    }*/
}
