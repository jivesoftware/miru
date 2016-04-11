package com.jivesoftware.os.miru.stream.plugins.catwalk;

import com.google.common.base.Optional;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Lists;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multiset.Entry;
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
import com.jivesoftware.os.miru.stream.plugins.catwalk.CatwalkQuery.CatwalkFeature;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Arrays;
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
        MiruPartitionCoord coord,
        Optional<CatwalkReport> report,
        BM[] featureAnswers,
        MiruSolutionLog solutionLog) throws Exception {

        StackBuffer stackBuffer = new StackBuffer();

        if (log.isDebugEnabled()) {
            log.debug("Catwalk for answer={} request={}", Arrays.toString(featureAnswers), request);
        }
        //System.out.println("Number of matches: " + bitmaps.cardinality(answer));

        MiruFieldIndex<BM, IBM> primaryIndex = requestContext.getFieldIndexProvider().getFieldIndex(MiruFieldType.primary);

        CatwalkFeature[] features = request.query.features;
        @SuppressWarnings("unchecked")
        Multiset<Feature>[] featureValueSets = new Multiset[features.length];
        for (int i = 0; i < features.length; i++) {
            featureValueSets[i] = HashMultiset.create();
        }
        int[][] featureFieldIds = new int[features.length][];
        for (int i = 0; i < features.length; i++) {
            String[] featureField = features[i].featureFields;
            featureFieldIds[i] = new int[featureField.length];
            for (int j = 0; j < featureField.length; j++) {
                featureFieldIds[i][j] = requestContext.getSchema().getFieldId(featureField[j]);
            }
        }

        aggregateUtil.gatherFeatures(name,
            bitmaps,
            requestContext,
            streamBitmaps -> streamBitmaps.stream(0, -1, null, -1, featureAnswers),
            featureFieldIds,
            false,
            (streamIndex, lastId, answerTermId, answerScoredLastId, featureId, termIds) -> {
                if (featureId >= 0) {
                    featureValueSets[featureId].add(new Feature(termIds));
                }
                return true;
            },
            solutionLog,
            stackBuffer);

        long start = System.currentTimeMillis();

        @SuppressWarnings("unchecked")
        List<FeatureScore>[] featureScoreResults = new List[features.length];

        for (int i = 0; i < featureValueSets.length; i++) {
            Multiset<Feature> valueSet = featureValueSets[i];
            featureScoreResults[i] = Lists.newArrayListWithCapacity(valueSet.size());
            for (Entry<Feature> entry : valueSet.entrySet()) {
                int[] fieldIds = featureFieldIds[i];
                MiruTermId[] termIds = entry.getElement().termIds;

                List<MiruTxIndex<IBM>> ands = Lists.newArrayList();
                for (int j = 0; j < fieldIds.length; j++) {
                    ands.add(primaryIndex.get(name, fieldIds[j], termIds[j]));
                }
                BM bitmap = bitmaps.andTx(ands, stackBuffer);
                int numerator = entry.getCount();
                long denominator = bitmaps.cardinality(bitmap);
                if (numerator > denominator) {
                    log.warn("Catwalk computed numerator:{} denominator:{} for tenantId:{} partitionId:{} featureId:{} fieldIds:{} terms:{}",
                        numerator, denominator, coord.tenantId, coord.partitionId, i, Arrays.toString(fieldIds), Arrays.toString(termIds));
                }
                featureScoreResults[i].add(new FeatureScore(termIds, numerator, denominator));
            }
        }

        solutionLog.log(MiruSolutionLogLevel.INFO, "Gather cardinalities took {} ms", System.currentTimeMillis() - start);

        boolean resultsExhausted = request.query.timeRange.smallestTimestamp > requestContext.getTimeIndex().getLargestTimestamp();
        boolean resultsClosed = requestContext.isClosed();

        MiruTimeRange timeRange = new MiruTimeRange(
            requestContext.getTimeIndex().getSmallestTimestamp(),
            requestContext.getTimeIndex().getLargestTimestamp());

        long[] modelCounts = new long[featureAnswers.length];
        for (int i = 0; i < featureAnswers.length; i++) {
            modelCounts[i] = bitmaps.cardinality(featureAnswers[i]);
        }
        long totalCount = requestContext.getTimeIndex().lastId();
        CatwalkAnswer result = new CatwalkAnswer(featureScoreResults, modelCounts, totalCount, timeRange, resultsExhausted, resultsClosed);
        log.debug("result={}", result);
        return result;
    }

}
