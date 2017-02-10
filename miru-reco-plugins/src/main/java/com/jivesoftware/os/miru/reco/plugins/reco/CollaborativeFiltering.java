package com.jivesoftware.os.miru.reco.plugins.reco;

import com.google.common.base.Optional;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.query.filter.MiruValue;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.FieldMultiTermTxIndex;
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
    public <BM extends IBM, IBM> RecoAnswer collaborativeFiltering(String name,
        MiruSolutionLog solutionLog,
        MiruBitmaps<BM, IBM> bitmaps,
        TrackError trackError,
        MiruRequestContext<BM, IBM, ?> requestContext,
        MiruPartitionCoord coord,
        final MiruRequest<RecoQuery> request,
        Optional<RecoReport> report,
        IBM allMyActivity,
        IBM okActivity,
        List<MiruValue> removeDistincts) throws Exception {
        StackBuffer stackBuffer = new StackBuffer();

        log.debug("Get collaborative filtering for allMyActivity={} okActivity={} query={}", allMyActivity, okActivity, request);

        //TODO expose to query?
        int gatherBatchSize = 100;

        MiruSchema schema = requestContext.getSchema();
        int fieldId1 = schema.getFieldId(request.query.aggregateFieldName1);
        int fieldId2 = schema.getFieldId(request.query.aggregateFieldName2);
        int fieldId3 = schema.getFieldId(request.query.aggregateFieldName3);
        MiruFieldDefinition fieldDefinition3 = schema.getFieldDefinition(fieldId3);

        // myOkActivity: my activity restricted to what's ok
        BM myOkActivity = bitmaps.and(Arrays.asList(allMyActivity, okActivity));

        MiruFieldIndex<BM, IBM> primaryFieldIndex = requestContext.getFieldIndexProvider().getFieldIndex(MiruFieldType.primary);
        MiruTermComposer termComposer = requestContext.getTermComposer();

        // distinctParents: distinct parents <field1> that I've touched
        Set<MiruTermId> distinctParents = Sets.newHashSet();

        aggregateUtil.gather(name,
            bitmaps,
            requestContext,
            myOkActivity,
            fieldId1,
            gatherBatchSize,
            false,
            false,
            Optional.absent(),
            solutionLog,
            (lastId, termId, count) -> {
                distinctParents.add(termId);
                return true;
            },
            stackBuffer);

        log.debug("allField1Activity: fieldId={}", fieldId1);
        FieldMultiTermTxIndex<BM, IBM> field1MultiTermTxIndex = new FieldMultiTermTxIndex<>(name, primaryFieldIndex, fieldId1, -1);
        field1MultiTermTxIndex.setTermIds(distinctParents.toArray(new MiruTermId[distinctParents.size()]));
        BM allField1Activity = bitmaps.orMultiTx(field1MultiTermTxIndex, stackBuffer);

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
        aggregateUtil.stream(name,
            bitmaps,
            requestContext,
            otherOkField1Activity,
            Optional.<BM>absent(),
            fieldId2,
            gatherBatchSize,
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
            return composeAnswer(requestContext, request, fieldDefinition3, contributorHeap, stackBuffer);
        }

        // augment distinctParents with additional distinct parents <field3> for exclusion below
        if (removeDistincts != null) {
            Set<MiruTermId> removeDistinctTermIds = Sets.newHashSet();
            for (MiruValue distinct : removeDistincts) {
                removeDistinctTermIds.add(termComposer.compose(schema, fieldDefinition3, stackBuffer, distinct.parts));
            }

            distinctParents.addAll(removeDistinctTermIds);
        }

        MiruTermCount[] contributorTermCounts = contributorHeap.toArray(new MiruTermCount[contributorHeap.size()]);
        MiruTermId[] contributorTermIds = new MiruTermId[contributorTermCounts.length];
        for (int i = 0; i < contributorTermCounts.length; i++) {
            contributorTermIds[i] = contributorTermCounts[i].termId;
        }

        BM[] contributorBitmaps = bitmaps.createArrayOf(contributorTermCounts.length);
        FieldMultiTermTxIndex<BM, IBM> field2MultiTermTxIndex = new FieldMultiTermTxIndex<>(name, primaryFieldIndex, fieldId2, -1);
        field2MultiTermTxIndex.setTermIds(contributorTermIds);
        bitmaps.multiTx(field2MultiTermTxIndex, (index, lastId, contributorActivity) -> {
            if (bitmaps.supportsInPlace()) {
                bitmaps.inPlaceAnd(contributorActivity, okActivity);
            } else {
                contributorActivity = bitmaps.and(Arrays.asList(contributorActivity, okActivity));
            }
            contributorBitmaps[index] = contributorActivity;
        }, stackBuffer);

        Multiset<MiruTermId> scoredParents = HashMultiset.create();
        for (int i = 0; i < contributorBitmaps.length; i++) {
            if (contributorBitmaps[i] != null) {
                Set<MiruTermId> distinctContributorParents = Sets.newHashSet();
                aggregateUtil.gather(name,
                    bitmaps,
                    requestContext,
                    contributorBitmaps[i],
                    fieldId3,
                    gatherBatchSize,
                    false,
                    false,
                    Optional.absent(),
                    solutionLog,
                    (lastId, termId, count) -> {
                        distinctContributorParents.add(termId);
                        return true;
                    },
                    stackBuffer);

                distinctContributorParents.removeAll(distinctParents);

                for (MiruTermId parent : distinctContributorParents) {
                    scoredParents.add(parent, (int) contributorTermCounts[i].count);
                }
            }
        }

        final MinMaxPriorityQueue<MiruTermCount> scoredHeap = MinMaxPriorityQueue.orderedBy(highestCountComparator)
            .maximumSize(request.query.desiredNumberOfDistincts)
            .create();
        for (Multiset.Entry<MiruTermId> entry : scoredParents.entrySet()) {
            scoredHeap.add(new MiruTermCount(entry.getElement(), null, entry.getCount()));
        }

        return composeAnswer(requestContext, request, fieldDefinition3, scoredHeap, stackBuffer);
    }

    private <BM extends IBM, IBM> RecoAnswer composeAnswer(MiruRequestContext<BM, IBM, ?> requestContext,
        MiruRequest<RecoQuery> request,
        MiruFieldDefinition fieldDefinition,
        MinMaxPriorityQueue<MiruTermCount> heap,
        StackBuffer stackBuffer) throws Exception {

        MiruSchema schema = requestContext.getSchema();
        MiruTermComposer termComposer = requestContext.getTermComposer();
        List<Recommendation> results = new ArrayList<>();
        for (MiruTermCount result : heap) {
            MiruValue term = new MiruValue(termComposer.decompose(schema, fieldDefinition, stackBuffer, result.termId));
            results.add(new Recommendation(term, result.count));
        }
        log.debug("score: results.size={}", results.size());
        boolean resultsExhausted = request.query.timeRange.smallestTimestamp > requestContext.getTimeIndex().getLargestTimestamp();
        return new RecoAnswer(results, 1, resultsExhausted);
    }

}
