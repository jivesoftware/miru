package com.jivesoftware.os.miru.stream.plugins.strut;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.jivesoftware.os.filer.io.ByteArrayFiler;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.query.filter.MiruValue;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.cache.MiruPluginCacheProvider.CacheKeyValues;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.MiruTermComposer;
import com.jivesoftware.os.miru.plugin.solution.MiruAggregateUtil;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.stream.plugins.catwalk.CatwalkQuery;
import com.jivesoftware.os.miru.stream.plugins.catwalk.CatwalkQuery.CatwalkFeature;
import com.jivesoftware.os.miru.stream.plugins.strut.HotOrNot.Hotness;
import com.jivesoftware.os.miru.stream.plugins.strut.StrutModelCache.ModelScore;
import com.jivesoftware.os.miru.stream.plugins.strut.StrutQuery.Strategy;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.rcvs.marshall.api.UtilLexMarshaller;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
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

    private static class Feature {
        public final int featureId;
        public final MiruTermId[] featureTermIds;

        public Feature(int featureId, MiruTermId[] featureTermIds) {
            this.featureId = featureId;
            this.featureTermIds = featureTermIds;
        }

        public byte[] pack(StackBuffer stackBuffer) throws IOException {
            return pack(featureId, featureTermIds, stackBuffer);
        }

        public static byte[] pack(int featureId, MiruTermId[] featureTermIds, StackBuffer stackBuffer) throws IOException {
            ByteArrayFiler byteArrayFiler = new ByteArrayFiler();
            FilerIO.write(byteArrayFiler, UtilLexMarshaller.intToLex(featureId), "featureId");
            FilerIO.writeInt(byteArrayFiler, featureTermIds == null ? -1 : featureTermIds.length, "featureTermIdCount", stackBuffer);
            if (featureTermIds != null) {
                for (int i = 0; i < featureTermIds.length; i++) {
                    FilerIO.writeByteArray(byteArrayFiler, featureTermIds[i].getBytes(), "featureTermId", stackBuffer);
                }
            }
            return byteArrayFiler.getBytes();
        }

        public static Feature unpack(byte[] bytes, StackBuffer stackBuffer) throws IOException {
            ByteArrayFiler byteArrayFiler = new ByteArrayFiler(bytes);
            byte[] lexFeatureId = new byte[4];
            FilerIO.read(byteArrayFiler, lexFeatureId);
            int featureId = UtilLexMarshaller.intFromLex(lexFeatureId);
            int featureTermIdCount = FilerIO.readInt(byteArrayFiler, "featureTermIdCount", stackBuffer);
            MiruTermId[] featureTermIds = null;
            if (featureTermIdCount != -1) {
                featureTermIds = new MiruTermId[featureTermIdCount];
                for (int i = 0; i < featureTermIdCount; i++) {
                    featureTermIds[i] = new MiruTermId(FilerIO.readByteArray(byteArrayFiler, "featureTermId", stackBuffer));
                }
            }
            return new Feature(featureId, featureTermIds);
        }
    }

    public interface StrutBitmapStream<BM> {

        boolean stream(int streamIndex, int lastId, MiruTermId termId, int scoredToLastId, BM[] answers) throws Exception;
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
        CacheKeyValues termFeatureCache,
        StrutStream<BM> strutStream,
        HotStuff hotStuff,
        AtomicInteger totalPartitionCount,
        MiruSolutionLog solutionLog) throws Exception {

        long start = System.currentTimeMillis();
        StackBuffer stackBuffer = new StackBuffer();

        MiruSchema schema = requestContext.getSchema();

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

        int[] persistTermIdCount = { 0 };
        int[] persistHitCount = { 0 };
        int[] persistSaveCount = { 0 };

        StrutModel model = cache.get(coord.tenantId, catwalkId, modelId, coord.partitionId.getId(), catwalkQuery);
        if (model == null) {

            strutStream.stream((streamIndex, lastId, termId, scoredToLastId, answers) -> {
                boolean more = true;
                if (!hotStuff.steamStream(streamIndex, new Scored(lastId, termId, scoredToLastId, 0f, new float[numeratorsCount], null), false)) {
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

            @SuppressWarnings("unchecked")
            Set<MiruAggregateUtil.Feature>[] dedupeFeatures = new Set[featureFieldIds.length];
            for (int i = 0; i < dedupeFeatures.length; i++) {
                dedupeFeatures[i] = Sets.newHashSet();
            }

            float[][] scores = new float[numeratorsCount][catwalkFeatures.length];
            int[][] counts = new int[numeratorsCount][catwalkFeatures.length];
            @SuppressWarnings("unchecked")
            List<MiruTermId[]>[] featuredTermIds = new List[catwalkFeatures.length];
            aggregateUtil.gatherFeatures(name,
                bitmaps,
                requestContext,
                streamBitmaps -> {
                    return strutStream.stream((streamIndex, lastId, termId, scoredToLastId, answers) -> {
                        for (Set<MiruAggregateUtil.Feature> dedupe : dedupeFeatures) {
                            dedupe.clear();
                        }

                        int[] fromId = { 0 };
                        termFeatureCache.rangeScan(termId.getBytes(), null, null, (key, value) -> {
                            Feature feature = Feature.unpack(key, stackBuffer);
                            int featureId = feature.featureId;
                            if (featureId == -1) {
                                fromId[0] = FilerIO.bytesInt(value);
                                return true;
                            }

                            MiruTermId[] termIds = feature.featureTermIds;
                            dedupeFeatures[featureId].add(new MiruAggregateUtil.Feature(termIds));
                            persistHitCount[0]++;

                            ModelScore modelScore = model.score(featureId, termIds);
                            if (modelScore != null) {
                                float[] s = new float[modelScore.numerators.length];
                                for (int i = 0; i < s.length; i++) {
                                    s[i] = (float) modelScore.numerators[i] / modelScore.denominator;
                                    if (s[i] > 1.0f) {
                                        LOG.warn("Encountered score {} > 1.0 for answerTermId:{} numeratorIndex:{} featureId:{} termIds:{}",
                                            s, termId, i, featureId, Arrays.toString(termIds));
                                    }
                                    scores[i][featureId] = score(scores[i][featureId], s[i], featureScalars[featureId], featureStrategy);
                                    counts[i][featureId]++;
                                }

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
                            return true;
                        });
                        return streamBitmaps.stream(streamIndex, lastId, termId, scoredToLastId, answers, fromId[0], dedupeFeatures);
                    });
                },
                featureFieldIds,
                (streamIndex, lastId, answerTermId, answerScoredLastId, featureId, termIds) -> {
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
                        Scored scored = new Scored(lastId, answerTermId, answerScoredLastId, scaledScore, termScores, scoredFeatures);
                        if (!hotStuff.steamStream(streamIndex, scored, cacheable[0])) {
                            stopped = true;
                        }

                        List<byte[]> persistKeys = Lists.newArrayList();
                        for (int i = 0; i < featuredTermIds.length; i++) {
                            if (featuredTermIds[i] != null) {
                                for (MiruTermId[] feature : featuredTermIds[i]) {
                                    persistKeys.add(Feature.pack(i, feature, stackBuffer));
                                }
                            }
                        }
                        if (!persistKeys.isEmpty()) {
                            persistTermIdCount[0]++;
                            persistSaveCount[0] += persistKeys.size();

                            byte[][] persistKeyBytes = persistKeys.toArray(new byte[0][]);
                            byte[][] persistValueBytes = new byte[persistKeyBytes.length][];
                            Arrays.fill(persistValueBytes, new byte[0]);

                            termFeatureCache.put(answerTermId.getBytes(), persistKeyBytes, persistValueBytes, false, false, stackBuffer);
                            termFeatureCache.put(answerTermId.getBytes(),
                                new byte[][] { Feature.pack(-1, null, stackBuffer) },
                                new byte[][] { FilerIO.intBytes(answerScoredLastId) },
                                false,
                                false,
                                stackBuffer);
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
                        ModelScore modelScore = model.score(featureId, termIds);
                        if (modelScore != null) {
                            float[] s = new float[modelScore.numerators.length];
                            for (int i = 0; i < s.length; i++) {
                                s[i] = (float) modelScore.numerators[i] / modelScore.denominator;
                                if (s[i] > 1.0f) {
                                    LOG.warn("Encountered score {} > 1.0 for answerTermId:{} numeratorIndex:{} featureId:{} termIds:{}",
                                        s, answerTermId, i, featureId, Arrays.toString(termIds));
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

        LOG.info("Strut scored {} features in {} ms, termIds={} hits={} saves={}",
            featureCount[0], System.currentTimeMillis() - start, persistTermIdCount[0], persistHitCount[0], persistSaveCount[0]);
        solutionLog.log(MiruSolutionLogLevel.INFO, "Strut scored {} features in {} ms, termIds={} hits={} saves={}",
            featureCount[0], System.currentTimeMillis() - start, persistTermIdCount[0], persistHitCount[0], persistSaveCount[0]);

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

    static class Scored implements Comparable<Scored> {

        int lastId;
        MiruTermId term;
        int scoredToLastId;
        float scaledScore;
        float[] scores;
        List<Hotness>[] features;

        public Scored(int lastId,
            MiruTermId term,
            int scoredToLastId,
            float scaledScore,
            float[] scores,
            List<Hotness>[] features) {

            this.lastId = lastId;
            this.term = term;
            this.scaledScore = scaledScore;
            this.scoredToLastId = scoredToLastId;
            this.scores = scores;
            this.features = features;
        }

        @Override
        public int compareTo(Scored o) {
            int c = Float.compare(o.scaledScore, scaledScore); // reversed
            if (c != 0) {
                return c;
            }
            c = Integer.compare(o.lastId, lastId); // reversed
            if (c != 0) {
                return c;
            }
            return term.compareTo(o.term);
        }

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
