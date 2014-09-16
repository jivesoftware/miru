package com.jivesoftware.os.miru.reco.plugins.reco;

import com.google.common.base.Optional;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.hash.Hashing;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.query.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.query.bitmap.MiruIntIterator;
import com.jivesoftware.os.miru.query.context.MiruRequestContext;
import com.jivesoftware.os.miru.query.index.BloomIndex;
import com.jivesoftware.os.miru.query.index.MiruField;
import com.jivesoftware.os.miru.query.index.MiruIndexUtil;
import com.jivesoftware.os.miru.query.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.query.solution.MiruAggregateUtil;
import com.jivesoftware.os.miru.query.solution.MiruRequest;
import com.jivesoftware.os.miru.query.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.query.solution.MiruTermCount;
import com.jivesoftware.os.miru.reco.plugins.reco.RecoAnswer.Recommendation;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.commons.lang.mutable.MutableInt;

/**
 *
 */
public class CollaborativeFiltering {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final MiruAggregateUtil aggregateUtil;
    private final MiruIndexUtil indexUtil;

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
            BM answer)
            throws Exception {

        log.debug("Get collaborative filtering for answer={} query={}", answer, request);

        BM contributors = possibleContributors(bitmaps, requestContext, request, answer);
        if (solutionLog.isEnabled()) {
            solutionLog.log("constributors " + bitmaps.cardinality(contributors) + ".");
        }
        BM otherContributors = bitmaps.create();
        bitmaps.andNot(otherContributors, contributors, Collections.singletonList(answer));
        // at this point we have all activity for all my viewed documents in 'contributors', and all activity not my own in 'otherContributors'.
        MinMaxPriorityQueue<MiruTermCount> contributorHeap = rankContributors(bitmaps, request, requestContext, otherContributors);
        if (solutionLog.isEnabled()) {
            solutionLog.log("not my self " + bitmaps.cardinality(otherContributors) + ".");
        }

        BM contributions = contributions(bitmaps, contributorHeap, requestContext, request.query);
        if (solutionLog.isEnabled()) {
            solutionLog.log("contributions " + bitmaps.cardinality(contributions) + ".");
        }
        final List<MiruTermCount> mostLike = new ArrayList<>(contributorHeap);
        final BloomIndex<BM> bloomIndex = new BloomIndex<>(bitmaps, Hashing.murmur3_128(), 100000, 0.01f); // TODO fix somehow
        final List<BloomIndex.Mights<MiruTermCount>> wantBits = bloomIndex.wantBits(mostLike);
        // TODO handle authz
        BM othersContributions = bitmaps.create();
        bitmaps.andNot(othersContributions, contributions, Collections.singletonList(contributors)); // remove activity for my viewed documents
        if (solutionLog.isEnabled()) {
            solutionLog.log("scorable " + bitmaps.cardinality(othersContributions) + ".");
        }
        return score(bitmaps, request, othersContributions, requestContext, bloomIndex, wantBits);

    }

    private <BM> BM contributions(MiruBitmaps<BM> bitmaps, MinMaxPriorityQueue<MiruTermCount> userHeap, MiruRequestContext<BM> requestContext, RecoQuery query)
            throws Exception {
        final MiruField<BM> lookupField2 = requestContext.fieldIndex.getField(requestContext.schema.getFieldId(query.lookupFieldNamed2));

        List<BM> toBeORed = new ArrayList<>();
        for (MiruTermCount tc : userHeap) {
            Optional<MiruInvertedIndex<BM>> invertedIndex = lookupField2.getInvertedIndex(indexUtil.makeComposite(tc.termId, "^", query.aggregateFieldName3));
            if (invertedIndex.isPresent()) {
                toBeORed.add(invertedIndex.get().getIndex());
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
        int fieldId = requestContext.schema.getFieldId(request.query.aggregateFieldName2);
        log.debug("rankContributors: fieldId={}", fieldId);
        MiruField<BM> aggregateField2 = requestContext.fieldIndex.getField(fieldId);

        final MinMaxPriorityQueue<MiruTermCount> userHeap = MinMaxPriorityQueue.orderedBy(new Comparator<MiruTermCount>() {

            @Override
            public int compare(MiruTermCount o1, MiruTermCount o2) {
                return -Long.compare(o1.count, o2.count); // minus to reverse :)
            }
        }).maximumSize(request.query.desiredNumberOfDistincts).create(); // overloaded :(

        aggregateUtil.stream(bitmaps, request.tenantId, requestContext, join1, Optional.<BM>absent(), aggregateField2, request.query.retrieveFieldName2,
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

    private <BM> BM possibleContributors(MiruBitmaps<BM> bitmaps, MiruRequestContext<BM> requestContext, final MiruRequest<RecoQuery> request, BM answer) throws
            Exception {
        MiruField<BM> aggregateField1 = requestContext.fieldIndex.getField(requestContext.schema.getFieldId(request.query.aggregateFieldName1));
        // feeds us our docIds
        List<BM> toBeORed = new ArrayList<>();
        MiruIntIterator answerIterator = bitmaps.intIterator(answer);
        int fieldId = requestContext.schema.getFieldId(request.query.aggregateFieldName1);
        log.debug("possibleContributors: fieldId={}", fieldId);
        while (answerIterator.hasNext()) {
            int id = answerIterator.next();
            MiruTermId[] fieldValues = requestContext.activityIndex.get(request.tenantId, id, fieldId);
            log.trace("possibleContributors: fieldValues={}", (Object) fieldValues);
            if (fieldValues != null && fieldValues.length > 0) {
                Optional<MiruInvertedIndex<BM>> invertedIndex = aggregateField1.getInvertedIndex(
                        indexUtil.makeComposite(fieldValues[0], "^", request.query.aggregateFieldName2));
                if (invertedIndex.isPresent()) {
                    toBeORed.add(invertedIndex.get().getIndex());
                }
            }
        }
        BM r = bitmaps.create();
        log.debug("possibleContributors: toBeORed.size={}", toBeORed.size());
        bitmaps.or(r, toBeORed);
        log.trace("possibleContributors: r={}", r);
        return r;
    }

    private <BM> RecoAnswer score(MiruBitmaps<BM> bitmaps, final MiruRequest<RecoQuery> request, BM join2, MiruRequestContext<BM> requestContext,
            final BloomIndex<BM> bloomIndex, final List<BloomIndex.Mights<MiruTermCount>> wantBits) throws Exception {

        int fieldId = requestContext.schema.getFieldId(request.query.aggregateFieldName3);
        log.debug("score: fieldId={}", fieldId);
        final MiruField<BM> aggregateField3 = requestContext.fieldIndex.getField(fieldId);

        final MinMaxPriorityQueue<MiruTermCount> heap = MinMaxPriorityQueue.orderedBy(new Comparator<MiruTermCount>() {

            @Override
            public int compare(MiruTermCount o1, MiruTermCount o2) {
                return -Long.compare(o1.count, o2.count); // minus to reverse :)
            }
        }).maximumSize(request.query.desiredNumberOfDistincts).create();
        // feeds us all recommended documents
        aggregateUtil.stream(bitmaps, request.tenantId, requestContext, join2, Optional.<BM>absent(), aggregateField3, request.query.retrieveFieldName3,
                new CallbackStream<MiruTermCount>() {
                    @Override
                    public MiruTermCount callback(MiruTermCount v) throws Exception {
                        if (v != null) {
                            MiruTermId[] fieldValues = v.mostRecent;
                            log.trace("score.fieldValues={}", (Object) fieldValues);
                            if (fieldValues != null && fieldValues.length > 0) {
                                Optional<MiruInvertedIndex<BM>> invertedIndex = aggregateField3.getInvertedIndex(
                                        indexUtil.makeComposite(fieldValues[0], "|", request.query.retrieveFieldName2));
                                if (invertedIndex.isPresent()) {
                                    MiruInvertedIndex<BM> index = invertedIndex.get();
                                    final MutableInt count = new MutableInt(0);
                                    bloomIndex.mightContain(index, wantBits, new BloomIndex.MightContain<MiruTermCount>() {

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

        List<Recommendation> results = new ArrayList<>();
        for (MiruTermCount result : heap) {
            results.add(new Recommendation(result.termId, result.count));
        }
        log.debug("score: results.size={}", results.size());
        return new RecoAnswer(results);
    }

}
