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

        MinMaxPriorityQueue<Scored> scored = MinMaxPriorityQueue
            .expectedSize(request.query.desiredNumberOfResults)
            .maximumSize(request.query.desiredNumberOfResults)
            .create();

        @SuppressWarnings("unchecked")
        List<Hotness>[] features = request.query.includeFeatures ? new List[featureFields.length] : null;

        long start = System.currentTimeMillis();
        int[] featureCount = { 0 };
        float[] score = { 0 };
        int[] termCount = { 0 };
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
                        if (termCount[0] > 0) {
                            List<Hotness>[] scoredFeatures = null;
                            if (request.query.includeFeatures) {
                                scoredFeatures = new List[features.length];
                                System.arraycopy(features, 0, scoredFeatures, 0, features.length);
                            }
                            scored.add(new Scored(currentPivot[0],
                                finalizeScore(score[0], termCount[0], request.query.strategy),
                                termCount[0],
                                scoredFeatures));
                        }
                        score[0] = 0f;
                        termCount[0] = 0;

                        if (request.query.includeFeatures) {
                            Arrays.fill(features, null);
                        }
                    }
                    currentPivot[0] = answerTermId;
                }
                float s = model.score(featureId, termIds, 0f);
                if (!Float.isNaN(s)) {
                    if (s > 1.0) {
                        LOG.warn("Encountered score {} > 1.0 for answerTermId:{} featureId:{} termIds:{}",
                            s, answerTermId, featureId, Arrays.toString(termIds));
                    }
                    //TODO tiered scoring based on thresholds
                    score[0] = score(score[0], s, request.query.strategy);
                    termCount[0]++;

                    if (request.query.includeFeatures) {
                        if (features[featureId] == null) {
                            features[featureId] = Lists.newArrayList();
                        }
                        MiruValue[] values = new MiruValue[termIds.length];
                        for (int i = 0; i < termIds.length; i++) {
                            values[i] = new MiruValue(termComposer.decompose(schema,
                                schema.getFieldDefinition(featureFieldIds[featureId][i]), stackBuffer, termIds[i]));
                        }
                        features[featureId].add(new Hotness(values, s));
                    }
                }
                return true;
            },
            solutionLog,
            stackBuffer);

        if (termCount[0] > 0) {
            scored.add(new Scored(currentPivot[0], finalizeScore(score[0], termCount[0], request.query.strategy), termCount[0], features));
        }

        solutionLog.log(MiruSolutionLogLevel.INFO, "Strut scored {} features for {} terms in {} ms",
            featureCount[0], scored.size(), System.currentTimeMillis() - start);

        for (Scored s : scored) {
            hotOrNots.add(new HotOrNot(new MiruValue(termComposer.decompose(schema, pivotFieldDefinition, stackBuffer, s.term)),
                s.score, s.termCount, s.features));
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
