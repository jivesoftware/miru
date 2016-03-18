package com.jivesoftware.os.miru.stream.plugins.strut;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.MinMaxPriorityQueue;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
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
import com.jivesoftware.os.miru.stream.plugins.strut.HotOrNot.Hotness;
import com.jivesoftware.os.miru.stream.plugins.strut.StrutModelCache.StrutModel;
import com.jivesoftware.os.miru.stream.plugins.strut.StrutQuery.Strategy;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
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

    public <BM extends IBM, IBM> StrutAnswer yourStuff(String name,
        MiruPartitionCoord coord,
        MiruBitmaps<BM, IBM> bitmaps,
        MiruRequestContext<BM, IBM, ?> requestContext,
        MiruRequest<StrutQuery> request,
        Optional<StrutReport> report,
        ConsumeBitmaps<BM> consumeAnswers,
        MiruSolutionLog solutionLog) throws Exception {

        StrutModel model = cache.get(request.tenantId, request.query.catwalkId, request.query.modelId, coord.partitionId.getId(), request.query.catwalkQuery);

        StackBuffer stackBuffer = new StackBuffer();

        MiruSchema schema = requestContext.getSchema();
        int pivotFieldId = schema.getFieldId(request.query.constraintField);
        MiruFieldDefinition pivotFieldDefinition = schema.getFieldDefinition(pivotFieldId);

        MiruTermComposer termComposer = requestContext.getTermComposer();
        String[][] modelFeatureFields = request.query.catwalkQuery.featureFields;
        String[][] desiredFeatureFields = request.query.featureFields;
        String[][] featureFields = new String[modelFeatureFields.length][];

        for (int i = 0; i < modelFeatureFields.length; i++) {
            for (int j = 0; j < desiredFeatureFields.length; j++) {
                if (Arrays.equals(modelFeatureFields[i], desiredFeatureFields[j])) {
                    featureFields[i] = modelFeatureFields[i];
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

        List<HotOrNot> hotOrNots = new ArrayList<>(request.query.desiredNumberOfResults);

        float[] thresholds = { 0.5f, 0.2f, 0.08f, 0f };

        @SuppressWarnings("unchecked")
        List<Hotness>[][] features = request.query.includeFeatures ? new List[thresholds.length][] : null;
        @SuppressWarnings("unchecked")
        MinMaxPriorityQueue<Scored>[] scored = new MinMaxPriorityQueue[thresholds.length];

        for (int i = 0; i < thresholds.length; i++) {
            if (features != null) {
                features[i] = new List[featureFields.length];
            }
            scored[i] = MinMaxPriorityQueue
                .expectedSize(request.query.desiredNumberOfResults)
                .maximumSize(request.query.desiredNumberOfResults)
                .create();
        }

        long start = System.currentTimeMillis();
        int[] featureCount = { 0 };
        float[] score = new float[thresholds.length];
        int[] termCount = new int[thresholds.length];
        MiruTermId[] currentPivot = { null };
        aggregateUtil.gatherFeatures(name,
            bitmaps,
            requestContext,
            consumeAnswers,
            featureFieldIds,
            true,
            (answerTermId, featureId, termIds) -> {
                featureCount[0]++;
                if (currentPivot[0] == null || !currentPivot[0].equals(answerTermId)) {
                    if (currentPivot[0] != null) {
                        for (int i = 0; i < thresholds.length; i++) {
                            if (termCount[i] > 0) {
                                List<Hotness>[] scoredFeatures = null;
                                if (request.query.includeFeatures) {
                                    scoredFeatures = new List[features[i].length];
                                    System.arraycopy(features[i], 0, scoredFeatures, 0, features[i].length);
                                }
                                scored[i].add(new Scored(currentPivot[0],
                                    finalizeScore(score[i], termCount[i], request.query.strategy),
                                    termCount[i],
                                    scoredFeatures));
                            }
                            score[i] = 0f;
                            termCount[i] = 0;

                            if (request.query.includeFeatures) {
                                Arrays.fill(features[i], null);
                            }
                        }
                    }
                    currentPivot[0] = answerTermId;
                }
                float s = model.score(featureId, termIds, Float.NaN);
                if (!Float.isNaN(s) && s > 0.0f) {
                    if (s > 1.0f) {
                        LOG.warn("Encountered score {} > 1.0 for answerTermId:{} featureId:{} termIds:{}",
                            s, answerTermId, featureId, Arrays.toString(termIds));
                    }
                    //TODO tiered scoring based on thresholds
                    for (int i = 0; i < thresholds.length; i++) {
                        if (s > thresholds[i]) {
                            score[i] = score(score[i], s, request.query.strategy);
                            termCount[i]++;

                            if (request.query.includeFeatures) {
                                if (features[i][featureId] == null) {
                                    features[i][featureId] = Lists.newArrayList();
                                }
                                MiruValue[] values = new MiruValue[termIds.length];
                                for (int j = 0; j < termIds.length; j++) {
                                    values[j] = new MiruValue(termComposer.decompose(schema,
                                        schema.getFieldDefinition(featureFieldIds[featureId][j]), stackBuffer, termIds[j]));
                                }
                                features[i][featureId].add(new Hotness(values, s));
                            }
                        }
                    }
                }
                return true;
            },
            solutionLog,
            stackBuffer);

        for (int i = 0; i < thresholds.length; i++) {
            if (termCount[i] > 0) {
                scored[i].add(new Scored(currentPivot[0],
                    finalizeScore(score[i], termCount[i], request.query.strategy),
                    termCount[i],
                    request.query.includeFeatures ? features[i] : null));
            }
        }

        solutionLog.log(MiruSolutionLogLevel.INFO, "Strut scored {} features in {} ms",
            featureCount[0], System.currentTimeMillis() - start);

        for (int i = 0; i < scored.length; i++) {
            if (i == scored.length - 1 || scored[i].size() == request.query.desiredNumberOfResults) {
                for (Scored s : scored[i]) {
                    hotOrNots.add(new HotOrNot(new MiruValue(termComposer.decompose(schema, pivotFieldDefinition, stackBuffer, s.term)),
                        s.score, s.termCount, s.features));
                }
                solutionLog.log(MiruSolutionLogLevel.INFO, "Strut found {} terms at threshold {}", hotOrNots.size(), thresholds[i]);
                break;
            }
        }

        boolean resultsExhausted = request.query.timeRange.smallestTimestamp > requestContext.getTimeIndex().getLargestTimestamp();

        return new StrutAnswer(hotOrNots, resultsExhausted);
    }

    private float score(float currentScore, float nextScore, Strategy strategy) {
        if (strategy == Strategy.MAX) {
            return Math.max(currentScore, nextScore);
        } else if (strategy == Strategy.MEAN) {
            return currentScore + nextScore;
        }
        throw new UnsupportedOperationException("Strategy not supported: " + strategy);
    }

    private float finalizeScore(float score, int termCount, Strategy strategy) {
        if (strategy == Strategy.MAX) {
            return score;
        } else if (strategy == Strategy.MEAN) {
            return score / termCount;
        }
        throw new UnsupportedOperationException("Strategy not supported: " + strategy);
    }

    public static void main(String[] args) {
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

        /*System.out.println(pViewed1);
        System.out.println(pNonViewed1);*/
        System.out.println(pViewed1 / p1);
        System.out.println(pNonViewed1 / p1);
        System.out.println("---");
        System.out.println(pViewed2 / pNonViewed2);
        System.out.println((pViewed1 * pViewed2) / (pNonViewed1 * pNonViewed2));

        /*System.out.println("" + ((7f / 10_000) * (3f / 10_000) * (8f / 10_000) * (10_000f / 3_000_000))
            / ((10f / 3_000_000) * (15f / 3_000_000) * (20f / 3_000_000)));

        System.out.println("" + ((3f / 2_990_000) * (12f / 2_990_000) * (12f / 2_990_000) * (2_990_000f / 3_000_000))
            / ((10f / 3_000_000) * (15f / 3_000_000) * (20f / 3_000_000)));*/
    }

    static class Scored implements Comparable<Scored> {

        MiruTermId term;
        float score;
        int termCount;
        List<Hotness>[] features;

        public Scored(MiruTermId term, float score, int termCount, List<Hotness>[] features) {
            this.term = term;
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
            return term.compareTo(o.term);
        }

    }
}
