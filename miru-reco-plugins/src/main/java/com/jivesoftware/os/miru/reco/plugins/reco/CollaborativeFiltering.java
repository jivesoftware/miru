package com.jivesoftware.os.miru.reco.plugins.reco;

import com.google.common.base.Optional;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruIndexUtil;
import com.jivesoftware.os.miru.plugin.index.MiruTermComposer;
import com.jivesoftware.os.miru.plugin.partition.TrackError;
import com.jivesoftware.os.miru.plugin.solution.MiruAggregateUtil;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.plugin.solution.MiruTermCount;
import com.jivesoftware.os.miru.reco.plugins.reco.RecoAnswer.Recommendation;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

/**
 *
 */
public class CollaborativeFiltering {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final MiruAggregateUtil aggregateUtil;
    private final MiruIndexUtil indexUtil;

    private final Comparator<MiruTermCount> highestCountComparator = (o1, o2) -> {
        return -Long.compare(o1.count, o2.count); // minus to reverse :)
    };

    public CollaborativeFiltering(MiruAggregateUtil aggregateUtil, MiruIndexUtil indexUtil) {
        this.aggregateUtil = aggregateUtil;
        this.indexUtil = indexUtil;
    }

    /*
     * I have viewed these things; among others who have also viewed these things, what have they viewed that I have not?
     */
    public <BM extends IBM, IBM> RecoAnswer collaborativeFiltering(MiruSolutionLog solutionLog,
        MiruBitmaps<BM, IBM> bitmaps,
        TrackError trackError,
        MiruRequestContext<BM, IBM, ?> requestContext,
        MiruPartitionCoord coord,
        final MiruRequest<RecoQuery> request,
        Optional<RecoReport> report,
        IBM allMyActivity,
        IBM okActivity,
        MiruFilter removeDistinctsFilter) throws Exception {
        StackBuffer stackBuffer = new StackBuffer();

        log.debug("Get collaborative filtering for allMyActivity={} okActivity={} query={}", allMyActivity, okActivity, request);

        //TODO expose to query?
        int gatherBatchSize = 100;

        int fieldId1 = requestContext.getSchema().getFieldId(request.query.aggregateFieldName1);
        int fieldId2 = requestContext.getSchema().getFieldId(request.query.aggregateFieldName2);
        int fieldId3 = requestContext.getSchema().getFieldId(request.query.aggregateFieldName3);
        MiruFieldDefinition fieldDefinition3 = requestContext.getSchema().getFieldDefinition(fieldId3);

        // myOkActivity: my activity restricted to what's ok
        BM myOkActivity = bitmaps.and(Arrays.asList(allMyActivity, okActivity));

        MiruFieldIndex<BM, IBM> primaryFieldIndex = requestContext.getFieldIndexProvider().getFieldIndex(MiruFieldType.primary);

        // distinctParents: distinct parents <field1> that I've touched
        Set<MiruTermId> distinctParents = Sets.newHashSet();

        aggregateUtil.gather(bitmaps, requestContext, myOkActivity, fieldId1, gatherBatchSize, solutionLog, termId -> {
            distinctParents.add(termId);
            return true;
        }, stackBuffer);

//        int[] indexes = bitmaps.indexes(myOkActivity);
//        List<MiruTermId[]> allFieldValues = requestContext.getActivityIndex().getAll(request.tenantId, indexes, fieldId1);
//        for (MiruTermId[] fieldValues : allFieldValues) {
//            Collections.addAll(distinctParents, fieldValues);
//        }
        List<IBM> toBeORed = new ArrayList<>();
        log.debug("allField1Activity: fieldId={}", fieldId1);
        for (MiruTermId parent : distinctParents) {
            Optional<BM> index = primaryFieldIndex.get(
                fieldId1,
                parent)
                .getIndex(stackBuffer);
            if (index.isPresent()) {
                toBeORed.add(index.get());
            }
        }

        // allField1Activity: all activity for the distinct parents <field1> that I've touched
        log.debug("allField1Activity: toBeORed.size={}", toBeORed.size());
        BM allField1Activity = bitmaps.or(toBeORed);
        log.trace("allField1Activity: allField1Activity={}", allField1Activity);
        if (solutionLog.isLogLevelEnabled(MiruSolutionLogLevel.INFO)) {
            solutionLog.log(MiruSolutionLogLevel.INFO, "allField1Activity {}.", bitmaps.cardinality(allField1Activity));
            solutionLog.log(MiruSolutionLogLevel.TRACE, "allField1Activity bitmap {}", allField1Activity);
        }

        BM okField1Activity = bitmaps.and(Arrays.asList(okActivity, allField1Activity));

        // otherOkField1Activity: all activity *except mine* for the distinct parents <field1>
        BM otherOkField1Activity = bitmaps.andNot(okField1Activity, myOkActivity);
        log.trace("otherOkField1Activity: otherOkField1Activity={}", otherOkField1Activity);
        if (solutionLog.isLogLevelEnabled(MiruSolutionLogLevel.INFO)) {
            solutionLog.log(MiruSolutionLogLevel.INFO, "otherOkField1Activity {}.", bitmaps.cardinality(otherOkField1Activity));
            solutionLog.log(MiruSolutionLogLevel.TRACE, "otherOkField1Activity bitmap {}", otherOkField1Activity);
        }

        // contributorHeap: ranked users <field2> based on cardinality of interactions with distinct parents <field1>
        final MinMaxPriorityQueue<MiruTermCount> contributorHeap = MinMaxPriorityQueue.orderedBy(highestCountComparator)
            .maximumSize(request.query.desiredNumberOfDistincts) // overloaded :(
            .create();
        aggregateUtil.stream(bitmaps,
            trackError,
            requestContext,
            coord,
            otherOkField1Activity,
            Optional.<BM>absent(),
            fieldId2,
            request.query.aggregateFieldName2,
            stackBuffer,
            miruTermCount -> {
                if (miruTermCount != null) {
                    contributorHeap.add(miruTermCount);
                }
                return miruTermCount;
            });

        if (request.query.aggregateFieldName2.equals(request.query.aggregateFieldName3)) {
            // special case where the ranked users <field2> are the desired parents <field3>
            return composeAnswer(requestContext, request, fieldDefinition3, contributorHeap);
        }

        // augment distinctParents with additional distinct parents <field1> for exclusion below
        if (!MiruFilter.NO_FILTER.equals(removeDistinctsFilter)) {
            BM remove = aggregateUtil.filter(bitmaps, requestContext.getSchema(), requestContext.getTermComposer(), requestContext.getFieldIndexProvider(),
                removeDistinctsFilter, solutionLog, null, requestContext.getActivityIndex().lastId(stackBuffer), -1, stackBuffer);
            if (solutionLog.isLogLevelEnabled(MiruSolutionLogLevel.INFO)) {
                solutionLog.log(MiruSolutionLogLevel.INFO, "remove {}.", bitmaps.cardinality(remove));
                solutionLog.log(MiruSolutionLogLevel.TRACE, "remove bitmap {}", remove);
            }

            aggregateUtil.gather(bitmaps, requestContext, remove, fieldId3, gatherBatchSize, solutionLog, termId -> {
                distinctParents.add(termId);
                return true;
            }, stackBuffer);

//            int[] removeIndexes = bitmaps.indexes(remove);
//            List<MiruTermId[]> removeFieldValues = requestContext.getActivityIndex().getAll(request.tenantId, removeIndexes, fieldId3);
//            for (MiruTermId[] fieldValues : removeFieldValues) {
//                Collections.addAll(distinctParents, fieldValues);
//            }
        }

        Multiset<MiruTermId> scoredParents = HashMultiset.create();

        for (MiruTermCount tc : contributorHeap) {
            Optional<BM> index = primaryFieldIndex.get(
                fieldId2,
                tc.termId)
                .getIndex(stackBuffer);
            if (index.isPresent()) {
                IBM contributorAllActivity = index.get();
                BM contributorOkActivity = bitmaps.and(Arrays.asList(okActivity, contributorAllActivity));

                Set<MiruTermId> distinctContributorParents = Sets.newHashSet();
                aggregateUtil.gather(bitmaps, requestContext, contributorOkActivity, fieldId3, gatherBatchSize, solutionLog,
                    termId -> {
                        distinctContributorParents.add(termId);
                        return true;
                    },
                    stackBuffer);

//                int[] contributorIndexes = bitmaps.indexes(contributorOkActivity);
//                List<MiruTermId[]> contributorFieldValues = requestContext.getActivityIndex().getAll(request.tenantId, contributorIndexes, fieldId3);
//                for (MiruTermId[] fieldValues : contributorFieldValues) {
//                    Collections.addAll(distinctContributorParents, fieldValues);
//                }
                distinctContributorParents.removeAll(distinctParents);

                for (MiruTermId parent : distinctContributorParents) {
                    scoredParents.add(parent, (int) tc.count);
                }
            }
        }

        final MinMaxPriorityQueue<MiruTermCount> scoredHeap = MinMaxPriorityQueue.orderedBy(highestCountComparator)
            .maximumSize(request.query.desiredNumberOfDistincts)
            .create();
        for (Multiset.Entry<MiruTermId> entry : scoredParents.entrySet()) {
            scoredHeap.add(new MiruTermCount(entry.getElement(), null, entry.getCount()));
        }

        return composeAnswer(requestContext, request, fieldDefinition3, scoredHeap);
    }

    private <BM extends IBM, IBM> RecoAnswer composeAnswer(MiruRequestContext<BM, IBM, ?> requestContext,
        MiruRequest<RecoQuery> request,
        MiruFieldDefinition fieldDefinition,
        MinMaxPriorityQueue<MiruTermCount> heap) throws IOException, InterruptedException {

        MiruTermComposer termComposer = requestContext.getTermComposer();
        List<Recommendation> results = new ArrayList<>();
        for (MiruTermCount result : heap) {
            String term = termComposer.decompose(fieldDefinition, result.termId);
            results.add(new Recommendation(term, result.count));
        }
        log.debug("score: results.size={}", results.size());
        boolean resultsExhausted = request.query.timeRange.smallestTimestamp > requestContext.getTimeIndex().getLargestTimestamp();
        return new RecoAnswer(results, 1, resultsExhausted);
    }

}
