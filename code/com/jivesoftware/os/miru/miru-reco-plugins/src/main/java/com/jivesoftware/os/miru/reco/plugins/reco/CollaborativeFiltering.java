package com.jivesoftware.os.miru.reco.plugins.reco;

import com.google.common.base.Optional;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.MiruIntIterator;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.BloomIndex;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruIndexUtil;
import com.jivesoftware.os.miru.plugin.index.MiruTermComposer;
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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang.mutable.MutableInt;

/**
 *
 */
public class CollaborativeFiltering {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final MiruAggregateUtil aggregateUtil;
    private final MiruIndexUtil indexUtil;

    private final Comparator<MiruTermCount> highestCountComparator = new Comparator<MiruTermCount>() {
        @Override
        public int compare(MiruTermCount o1, MiruTermCount o2) {
            return -Long.compare(o1.count, o2.count); // minus to reverse :)
        }
    };

    public CollaborativeFiltering(MiruAggregateUtil aggregateUtil, MiruIndexUtil indexUtil) {
        this.aggregateUtil = aggregateUtil;
        this.indexUtil = indexUtil;
    }

    /*
     * I have viewed these things; among others who have also viewed these things, what have they viewed that I have not?
     */
    public <BM> RecoAnswer collaborativeFiltering(MiruSolutionLog solutionLog,
        MiruBitmaps<BM> bitmaps,
        MiruRequestContext<BM> requestContext,
        final MiruRequest<RecoQuery> request,
        Optional<RecoReport> report,
        BM allMyActivity,
        BM okActivity,
        MiruFilter removeDistinctsFilter)
        throws Exception {

        log.debug("Get collaborative filtering for allMyActivity={} allOkActivity={} query={}", allMyActivity, okActivity, request);

        int fieldId1 = requestContext.getSchema().getFieldId(request.query.aggregateFieldName1);
        int fieldId2 = requestContext.getSchema().getFieldId(request.query.aggregateFieldName2);
        int fieldId3 = requestContext.getSchema().getFieldId(request.query.aggregateFieldName3);
        MiruFieldDefinition fieldDefinition3 = requestContext.getSchema().getFieldDefinition(fieldId3);

        // myOkActivity: all my activity
        BM myOkActivity = bitmaps.create();
        bitmaps.and(myOkActivity, Arrays.asList(allMyActivity, okActivity));

        // distinctParents: distinct parents <field1> that I've touched
        Set<MiruTermId> distinctParents = Sets.newHashSet();
        MiruIntIterator iter = bitmaps.intIterator(myOkActivity);
        while (iter.hasNext()) {
            int id = iter.next();
            MiruTermId[] fieldValues = requestContext.getActivityIndex().get(request.tenantId, id, fieldId1); //TODO batch get by fieldId
            Collections.addAll(distinctParents, fieldValues);
        }

        MiruFieldIndex<BM> primaryFieldIndex = requestContext.getFieldIndexProvider().getFieldIndex(MiruFieldType.primary);
        List<BM> toBeORed = new ArrayList<>();
        log.debug("allField1Activity: fieldId={}", fieldId1);
        for (MiruTermId parent : distinctParents) {
            Optional<BM> index = primaryFieldIndex.get(
                fieldId1,
                parent)
                .getIndex();
            if (index.isPresent()) {
                toBeORed.add(index.get());
            }
        }

        // allField1Activity: all activity for the distinct parents <field1> that I've touched
        BM allField1Activity = bitmaps.create();
        log.debug("allField1Activity: toBeORed.size={}", toBeORed.size());
        bitmaps.or(allField1Activity, toBeORed);
        log.trace("allField1Activity: allField1Activity={}", allField1Activity);
        if (solutionLog.isLogLevelEnabled(MiruSolutionLogLevel.INFO)) {
            solutionLog.log(MiruSolutionLogLevel.INFO, "allField1Activity {}.", bitmaps.cardinality(allField1Activity));
            solutionLog.log(MiruSolutionLogLevel.TRACE, "allField1Activity bitmap {}", allField1Activity);
        }

        BM okField1Activity = bitmaps.create();
        bitmaps.and(okField1Activity, Arrays.asList(okActivity, allField1Activity));

        // otherOkField1Activity: all activity *except mine* for the distinct parents <field1>
        BM otherOkField1Activity = bitmaps.create();
        bitmaps.andNot(otherOkField1Activity, okField1Activity, Arrays.asList(myOkActivity));
        log.trace("otherOkField1Activity: otherOkField1Activity={}", otherOkField1Activity);
        if (solutionLog.isLogLevelEnabled(MiruSolutionLogLevel.INFO)) {
            solutionLog.log(MiruSolutionLogLevel.INFO, "otherOkField1Activity {}.", bitmaps.cardinality(otherOkField1Activity));
            solutionLog.log(MiruSolutionLogLevel.TRACE, "otherOkField1Activity bitmap {}", otherOkField1Activity);
        }

        // contributorHeap: ranked users <field2> based on cardinality of interactions with distinct parents <field1>
        final MinMaxPriorityQueue<MiruTermCount> contributorHeap = MinMaxPriorityQueue.orderedBy(highestCountComparator)
            .maximumSize(request.query.desiredNumberOfDistincts) // overloaded :(
            .create();
        aggregateUtil.stream(bitmaps, request.tenantId, requestContext, otherOkField1Activity, Optional.<BM>absent(), fieldId2,
            request.query.aggregateFieldName2, new CallbackStream<MiruTermCount>() {
                @Override
                public MiruTermCount callback(MiruTermCount miruTermCount) throws Exception {
                    if (miruTermCount != null) {
                        contributorHeap.add(miruTermCount);
                    }
                    return miruTermCount;
                }
            });

        if (request.query.aggregateFieldName2.equals(request.query.aggregateFieldName3)) {
            // special case where the ranked users <field2> are the desired parents <field3>
            return composeAnswer(requestContext, fieldDefinition3, contributorHeap);
        }

        // augment distinctParents with additional distinct parents <field1> for exclusion below
        if (!MiruFilter.NO_FILTER.equals(removeDistinctsFilter)) {
            BM remove = bitmaps.create();
            aggregateUtil.filter(bitmaps, requestContext.getSchema(), requestContext.getTermComposer(), requestContext.getFieldIndexProvider(),
                removeDistinctsFilter, solutionLog, remove, requestContext.getActivityIndex().lastId(), -1);
            if (solutionLog.isLogLevelEnabled(MiruSolutionLogLevel.INFO)) {
                solutionLog.log(MiruSolutionLogLevel.INFO, "remove {}.", bitmaps.cardinality(remove));
                solutionLog.log(MiruSolutionLogLevel.TRACE, "remove bitmap {}", remove);
            }

            MiruIntIterator removeIter = bitmaps.intIterator(remove);
            while (removeIter.hasNext()) {
                int id = removeIter.next();
                MiruTermId[] fieldValues = requestContext.getActivityIndex().get(request.tenantId, id, fieldId3); //TODO batch get by fieldId
                Collections.addAll(distinctParents, fieldValues);
            }
        }

        Multiset<MiruTermId> scoredParents = HashMultiset.create();

        for (MiruTermCount tc : contributorHeap) {
            Optional<BM> index = primaryFieldIndex.get(
                fieldId2,
                tc.termId)
                .getIndex();
            if (index.isPresent()) {
                BM contributorAllActivity = index.get();
                BM contributorOkActivity = bitmaps.create();
                bitmaps.and(contributorOkActivity, Arrays.asList(okActivity, contributorAllActivity));

                MiruIntIterator contributorIter = bitmaps.intIterator(contributorOkActivity);
                Set<MiruTermId> distinctContributorParents = Sets.newHashSet();
                while (contributorIter.hasNext()) {
                    int id = contributorIter.next();
                    MiruTermId[] fieldValues = requestContext.getActivityIndex().get(request.tenantId, id, fieldId3); //TODO batch get by fieldId
                    Collections.addAll(distinctContributorParents, fieldValues);
                }

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

        return composeAnswer(requestContext, fieldDefinition3, scoredHeap);
    }

    private <BM> BM contributions(MiruBitmaps<BM> bitmaps, MinMaxPriorityQueue<MiruTermCount> userHeap, MiruRequestContext<BM> requestContext, RecoQuery query)
        throws Exception {

        int fieldId = requestContext.getSchema().getFieldId(query.lookupFieldName2);
        MiruFieldIndex<BM> fieldIndex = requestContext.getFieldIndexProvider().getFieldIndex(MiruFieldType.pairedLatest);

        List<BM> toBeORed = new ArrayList<>();
        for (MiruTermCount tc : userHeap) {
            Optional<BM> index = fieldIndex.get(
                fieldId,
                indexUtil.makePairedLatestTerm(tc.termId, query.aggregateFieldName3))
                .getIndex();
            if (index.isPresent()) {
                toBeORed.add(index.get());
            }
        }
        BM r = bitmaps.create();
        bitmaps.or(r, toBeORed);
        return r;
    }

    private <BM> MinMaxPriorityQueue<MiruTermCount> rankContributors(MiruBitmaps<BM> bitmaps,
        final MiruRequest<RecoQuery> request,
        MiruRequestContext<BM> requestContext,
        BM join1)
        throws Exception {

        int fieldId = requestContext.getSchema().getFieldId(request.query.aggregateFieldName2);
        log.debug("rankContributors: fieldId={}", fieldId);

        final MinMaxPriorityQueue<MiruTermCount> userHeap = MinMaxPriorityQueue.orderedBy(highestCountComparator)
            .maximumSize(request.query.desiredNumberOfDistincts) // overloaded :(
            .create();

        aggregateUtil.stream(bitmaps, request.tenantId, requestContext, join1, Optional.<BM>absent(), fieldId, request.query.retrieveFieldName2,
            new CallbackStream<MiruTermCount>() {
                @Override
                public MiruTermCount callback(MiruTermCount v) throws Exception {
                    if (v != null) {
                        userHeap.add(v);
                    }
                    return v;
                }
            });
        return userHeap;
    }

    private <BM> BM possibleContributors(MiruBitmaps<BM> bitmaps, MiruRequestContext<BM> requestContext, final MiruRequest<RecoQuery> request, BM answer)
        throws Exception {

        int fieldId = requestContext.getSchema().getFieldId(request.query.aggregateFieldName1);
        MiruFieldIndex<BM> fieldIndex = requestContext.getFieldIndexProvider().getFieldIndex(MiruFieldType.pairedLatest);
        List<BM> toBeORed = new ArrayList<>();
        MiruIntIterator answerIterator = bitmaps.intIterator(answer);
        log.debug("possibleContributors: fieldId={}", fieldId);
        // feeds us our docIds
        while (answerIterator.hasNext()) {
            int id = answerIterator.next();
            MiruTermId[] fieldValues = requestContext.getActivityIndex().get(request.tenantId, id, fieldId);
            log.trace("possibleContributors: fieldValues={}", (Object) fieldValues);
            if (fieldValues != null && fieldValues.length > 0) {
                Optional<BM> index = fieldIndex.get(
                    fieldId,
                    indexUtil.makePairedLatestTerm(fieldValues[0], request.query.aggregateFieldName2))
                    .getIndex();
                if (index.isPresent()) {
                    toBeORed.add(index.get());
                }
            }
        }
        BM r = bitmaps.create();
        log.debug("possibleContributors: toBeORed.size={}", toBeORed.size());
        bitmaps.or(r, toBeORed);
        log.trace("possibleContributors: r={}", r);
        return r;
    }

    private <BM> RecoAnswer score(MiruBitmaps<BM> bitmaps, final MiruRequest<RecoQuery> request, BM scorable, MiruRequestContext<BM> requestContext,
        final BloomIndex<BM> bloomIndex, final List<BloomIndex.Mights<MiruTermCount>> wantBits) throws Exception {

        final int fieldId = requestContext.getSchema().getFieldId(request.query.aggregateFieldName3);
        MiruFieldDefinition fieldDefinition = requestContext.getSchema().getFieldDefinition(fieldId);
        log.debug("score: fieldId={}", fieldId);

        final MiruFieldIndex<BM> bloomFieldIndex = requestContext.getFieldIndexProvider().getFieldIndex(MiruFieldType.bloom);
        final MinMaxPriorityQueue<MiruTermCount> heap = MinMaxPriorityQueue.orderedBy(highestCountComparator)
            .maximumSize(request.query.desiredNumberOfDistincts)
            .create();

        // feeds us all recommended parents <field3>
        aggregateUtil.stream(bitmaps, request.tenantId, requestContext, scorable, Optional.<BM>absent(), fieldId, request.query.retrieveFieldName3,
            new CallbackStream<MiruTermCount>() {
                @Override
                public MiruTermCount callback(MiruTermCount v) throws Exception {
                    if (v != null) {
                        MiruTermId[] fieldValues = v.mostRecent;
                        log.trace("score.fieldValues={}", (Object) fieldValues);
                        if (fieldValues != null && fieldValues.length > 0) {
                            Optional<BM> index = bloomFieldIndex.get(
                                fieldId,
                                indexUtil.makeBloomTerm(fieldValues[0], request.query.retrieveFieldName2))
                                .getIndex();
                            final MutableInt count = new MutableInt(0);
                            if (index.isPresent()) {
                                bloomIndex.mightContain(index.get(), wantBits, new BloomIndex.MightContain<MiruTermCount>() {

                                    @Override
                                    public void mightContain(MiruTermCount value) {
                                        count.add(value.count);
                                    }
                                });
                                heap.add(new MiruTermCount(fieldValues[0], null, count.longValue()));

                                for (BloomIndex.Mights<MiruTermCount> boo : wantBits) {
                                    boo.reset();
                                }
                            }
                        }
                    }
                    return v;
                }
            });

        return composeAnswer(requestContext, fieldDefinition, heap);
    }

    private <BM> RecoAnswer composeAnswer(MiruRequestContext<BM> requestContext,
        MiruFieldDefinition fieldDefinition,
        MinMaxPriorityQueue<MiruTermCount> heap) {

        MiruTermComposer termComposer = requestContext.getTermComposer();
        List<Recommendation> results = new ArrayList<>();
        for (MiruTermCount result : heap) {
            String term = termComposer.decompose(fieldDefinition, result.termId);
            results.add(new Recommendation(term, result.count));
        }
        log.debug("score: results.size={}", results.size());
        return new RecoAnswer(results, 1);
    }

}
