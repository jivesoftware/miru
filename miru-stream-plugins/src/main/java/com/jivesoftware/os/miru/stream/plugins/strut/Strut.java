package com.jivesoftware.os.miru.stream.plugins.strut;

import com.google.common.base.Optional;
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
import com.jivesoftware.os.miru.stream.plugins.strut.StrutModelCache.StrutModel;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 *
 */
public class Strut {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();
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

        long start = System.currentTimeMillis();
        int[] featureCount = { 0 };
        float[] score = { 0 };
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
                        if (score[0] > 0) {
                            scored.add(new Scored(answerTermId, score[0]));
                        }
                        score[0] = 0f;
                    }
                    currentPivot[0] = answerTermId;
                }
                float s = model.score(featureId, termIds, 0f);
                if (!Float.isNaN(s)) {
                    score[0] = Math.max(score[0], s);
                }
                return true;
            },
            solutionLog,
            stackBuffer);

        if (score[0] > 0) {
            scored.add(new Scored(currentPivot[0], score[0]));
        }

        solutionLog.log(MiruSolutionLogLevel.INFO, "Strut scored {} features for {} terms in {} ms",
            featureCount[0], scored.size(), System.currentTimeMillis() - start);

        for (Scored s : scored) {
            hotOrNots.add(new HotOrNot(new MiruValue(termComposer.decompose(schema, pivotFieldDefinition, stackBuffer, s.term)), s.score));
        }

        boolean resultsExhausted = request.query.timeRange.smallestTimestamp > requestContext.getTimeIndex().getLargestTimestamp();

        return new StrutAnswer(hotOrNots, resultsExhausted);
    }

    static class Scored implements Comparable<Scored> {

        MiruTermId term;
        float score;

        public Scored(MiruTermId term, float score) {
            this.term = term;
            this.score = score;
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
