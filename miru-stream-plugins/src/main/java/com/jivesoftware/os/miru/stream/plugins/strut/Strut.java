package com.jivesoftware.os.miru.stream.plugins.strut;

import com.google.common.collect.Lists;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.query.filter.MiruValue;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.MiruTermComposer;
import com.jivesoftware.os.miru.plugin.solution.MiruAggregateUtil;
import com.jivesoftware.os.miru.plugin.solution.MiruAggregateUtil.ConsumeBitmaps;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.stream.plugins.catwalk.CatwalkQuery.CatwalkFeature;
import com.jivesoftware.os.miru.stream.plugins.strut.HotOrNot.Hotness;
import com.jivesoftware.os.miru.stream.plugins.strut.StrutModelCache.ModelScore;
import com.jivesoftware.os.miru.stream.plugins.strut.StrutQuery.Strategy;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Arrays;
import java.util.List;

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
        List<HotOrNot> hotOrNots) throws Exception {
        boolean resultsExhausted = request.query.timeRange.smallestTimestamp > requestContext.getTimeIndex().getLargestTimestamp();
        return new StrutAnswer(hotOrNots, resultsExhausted);
    }

    public interface HotStuff {

        boolean steamStream(int streamIndex, Scored scored, boolean cacheable);
    }

    public <BM extends IBM, IBM> void yourStuff(String name,
        MiruPartitionCoord coord,
        MiruBitmaps<BM, IBM> bitmaps,
        MiruRequestContext<BM, IBM, ?> requestContext,
        MiruRequest<StrutQuery> request,
        ConsumeBitmaps<BM> consumeAnswers,
        HotStuff hotStuff,
        MiruSolutionLog solutionLog) throws Exception {

        StackBuffer stackBuffer = new StackBuffer();

        MiruSchema schema = requestContext.getSchema();

        MiruTermComposer termComposer = requestContext.getTermComposer();
        CatwalkFeature[] catwalkFeatures = request.query.catwalkQuery.features;
        String[] strutFeatureNames = request.query.featureNames;

        String[][] featureFields = new String[catwalkFeatures.length][];
        for (int i = 0; i < catwalkFeatures.length; i++) {
            for (int j = 0; j < strutFeatureNames.length; j++) {
                if (catwalkFeatures[i].name.equals(strutFeatureNames[j])) {
                    featureFields[i] = catwalkFeatures[i].featureFields;
                    break;
                }
            }
        }

        int[][] featureFieldIds = new int[featureFields.length][];
        for (int i = 0; i < featureFields.length; i++) {
            String[] featureField = featureFields[i];
            if (featureField != null) {
                featureFieldIds[i] = new int[featureField.length];
                for (int j = 0; j < featureField.length; j++) {
                    featureFieldIds[i][j] = requestContext.getSchema().getFieldId(featureField[j]);
                }
            }
        }

        @SuppressWarnings("unchecked")
        List<Hotness>[] features = request.query.includeFeatures ? new List[featureFields.length] : null;

        long start = System.currentTimeMillis();
        int[] featureCount = {0};

        StrutModel model = cache.get(request.tenantId, request.query.catwalkId, request.query.modelId, coord.partitionId.getId(), request.query.catwalkQuery);
        if (model == null) {

            consumeAnswers.consume((streamIndex, lastId, termId, scoredToLastId, answers) -> {
                boolean more = true;
                if (!hotStuff.steamStream(streamIndex, new Scored(lastId, termId, scoredToLastId, 0.0f, 0, null), false)) {
                    more = false;
                }
                return more;
            });

        } else {
            boolean[] cacheable = new boolean[]{true};
            for (int i = 0; i < featureFields.length; i++) {
                if (model.numberOfModels[i] == 0) {
                    cacheable[0] = false;
                    break;
                }
            }

            float[] score = new float[1];
            int[] termCount = new int[1];
            aggregateUtil.gatherFeatures(name,
                bitmaps,
                requestContext,
                consumeAnswers,
                featureFieldIds,
                true,
                (streamIndex, lastId, answerTermId, answerScoredLastId, featureId, termIds) -> {
                    if (featureId == -1) {
                        boolean stopped = false;
                        if (termCount[0] > 0) {
                            List<Hotness>[] scoredFeatures = null;
                            if (request.query.includeFeatures) {
                                scoredFeatures = new List[features.length];
                                System.arraycopy(features, 0, scoredFeatures, 0, features.length);
                            }
                            float s = finalizeScore(score[0], termCount[0], request.query.strategy);
                            Scored scored = new Scored(lastId, answerTermId, answerScoredLastId, s, termCount[0], scoredFeatures);
                            if (!hotStuff.steamStream(streamIndex, scored, cacheable[0])) {
                                stopped = true;
                            }
                        } else if (!hotStuff.steamStream(streamIndex, new Scored(lastId, answerTermId, answerScoredLastId, 0f, 0, null), cacheable[0])) {
                            stopped = true;
                        }
                        score[0] = 0f;
                        termCount[0] = 0;

                        if (request.query.includeFeatures) {
                            Arrays.fill(features, null);
                        }
                        if (stopped) {
                            return false;
                        }

                    } else {
                        featureCount[0]++;
                        ModelScore modelScore = model.score(featureId, termIds);
                        if (modelScore != null) {
                            float s = (float) modelScore.numerator / modelScore.denominator;
                            if (s > 1.0f) {
                                LOG.warn("Encountered score {} > 1.0 for answerTermId:{} featureId:{} termIds:{}",
                                    s, answerTermId, featureId, Arrays.toString(termIds));
                            } else if (!Float.isNaN(s) && s > 0f) {
                                score[0] = score(score[0], s, request.query.strategy);
                                termCount[0]++;

                                if (request.query.includeFeatures) {
                                    if (features[featureId] == null) {
                                        features[featureId] = Lists.newArrayList();
                                    }
                                    MiruValue[] values = new MiruValue[termIds.length];
                                    for (int j = 0; j < termIds.length; j++) {
                                        values[j] = new MiruValue(termComposer.decompose(schema,
                                            schema.getFieldDefinition(featureFieldIds[featureId][j]), stackBuffer, termIds[j]));
                                    }
                                    features[featureId].add(new Hotness(values, s));
                                }
                            }
                        }
                    }
                    return true;
                },
                solutionLog,
                stackBuffer);

        }

        solutionLog.log(MiruSolutionLogLevel.INFO, "Strut scored {} features in {} ms", featureCount[0], System.currentTimeMillis() - start);

    }

    private float score(float score, float s, Strategy strategy) {
        if (strategy == Strategy.MAX) {
            return Math.max(score, s);
        } else if (strategy == Strategy.MEAN) {
            return score + s;
        } else {
            throw new UnsupportedOperationException("Strategy not supported: " + strategy);
        }
    }

    private float finalizeScore(float score, int termCount, Strategy strategy) {
        if (strategy == Strategy.MAX) {
            return score;
        } else if (strategy == Strategy.MEAN) {
            return score / termCount;
        } else {
            throw new UnsupportedOperationException("Strategy not supported: " + strategy);
        }
    }

    static class Scored implements Comparable<Scored> {

        int lastId;
        MiruTermId term;
        int scoredToLastId;
        float score;
        int termCount;
        List<Hotness>[] features;

        public Scored(int lastId, MiruTermId term, int scoredToLastId, float score, int termCount, List<Hotness>[] features) {
            this.lastId = lastId;
            this.term = term;
            this.scoredToLastId = scoredToLastId;
            this.score = score;
            this.termCount = termCount;
            this.features = features;
        }

        @Override
        public int compareTo(Scored o) {
            int c = Float.compare(o.score, score); // reversed
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
