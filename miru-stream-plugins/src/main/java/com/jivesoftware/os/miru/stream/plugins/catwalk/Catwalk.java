package com.jivesoftware.os.miru.stream.plugins.catwalk;

import com.google.common.base.Optional;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Lists;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multiset.Entry;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.query.filter.MiruValue;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruTermComposer;
import com.jivesoftware.os.miru.plugin.index.MiruTxIndex;
import com.jivesoftware.os.miru.plugin.solution.MiruAggregateUtil;
import com.jivesoftware.os.miru.plugin.solution.MiruAggregateUtil.Feature;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.stream.plugins.catwalk.CatwalkAnswer.FeatureScore;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.List;

/**
 *
 */
public class Catwalk {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final MiruAggregateUtil aggregateUtil = new MiruAggregateUtil();

    public <BM extends IBM, IBM> CatwalkAnswer model(String name,
        MiruBitmaps<BM, IBM> bitmaps,
        MiruRequestContext<BM, IBM, ?> requestContext,
        MiruRequest<CatwalkQuery> request,
        Optional<CatwalkReport> report,
        BM answer,
        MiruSolutionLog solutionLog) throws Exception {

        StackBuffer stackBuffer = new StackBuffer();

        log.debug("Catwalk for answer={} request={}", answer, request);
        //System.out.println("Number of matches: " + bitmaps.cardinality(answer));

        MiruSchema schema = requestContext.getSchema();
        MiruFieldIndex<BM, IBM> primaryIndex = requestContext.getFieldIndexProvider().getFieldIndex(MiruFieldType.primary);
        MiruTermComposer termComposer = requestContext.getTermComposer();

        String[][] featureFields = request.query.featureFields;
        @SuppressWarnings("unchecked")
        Multiset<Feature>[] featureValueSets = new Multiset[featureFields.length];
        for (int i = 0; i < featureFields.length; i++) {
            featureValueSets[i] = HashMultiset.create();
        }
        int[][] featureFieldIds = new int[featureFields.length][];
        for (int i = 0; i < featureFields.length; i++) {
            String[] featureField = featureFields[i];
            for (int j = 0; j < featureFields[i].length; j++) {
                featureFieldIds[i][j] = requestContext.getSchema().getFieldId(featureField[i]);
            }
        }
        aggregateUtil.gatherFeatures(name, bitmaps, requestContext, answer, featureFieldIds, false, (featureId, termIds) -> {
            featureValueSets[featureId].add(new Feature(termIds));
            return true;
        }, solutionLog, stackBuffer);

        long start = System.currentTimeMillis();

        @SuppressWarnings("unchecked")
        List<FeatureScore>[] featureScoreResults = new List[featureFields.length];

        for (int i = 0; i < featureValueSets.length; i++) {
            Multiset<Feature> valueSet = featureValueSets[i];
            featureScoreResults[i] = Lists.newArrayListWithCapacity(valueSet.size());
            for (Entry<Feature> entry : valueSet.entrySet()) {
                int[] fieldIds = featureFieldIds[i];
                MiruTermId[] termIds = entry.getElement().termIds;
                MiruValue[] featureValues = new MiruValue[fieldIds.length];
                for (int j = 0; j < fieldIds.length; j++) {
                    featureValues[j] = new MiruValue(termComposer.decompose(schema,
                        schema.getFieldDefinition(fieldIds[j]),
                        stackBuffer,
                        termIds[j]));
                }

                List<MiruTxIndex<IBM>> ands = Lists.newArrayList();
                for (int j = 0; j < fieldIds.length; j++) {
                    ands.add(primaryIndex.get(name, fieldIds[j], termIds[j]));
                }
                BM bitmap = bitmaps.andTx(ands, stackBuffer);
                featureScoreResults[i].add(new FeatureScore(featureValues, entry.getCount(), bitmaps.cardinality(bitmap)));
            }
        }

        solutionLog.log(MiruSolutionLogLevel.INFO, "Gather cardinalities took {} ms", System.currentTimeMillis() - start);

        boolean resultsExhausted = request.query.timeRange.smallestTimestamp > requestContext.getTimeIndex().getLargestTimestamp();

        CatwalkAnswer result = new CatwalkAnswer(featureScoreResults, resultsExhausted);
        log.debug("result={}", result);
        return result;
    }

}
