package com.jivesoftware.os.miru.reco.plugins.reco;

import com.google.common.base.Optional;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.hash.Hashing;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.query.BloomIndex;
import com.jivesoftware.os.miru.query.MiruBitmaps;
import com.jivesoftware.os.miru.query.MiruField;
import com.jivesoftware.os.miru.query.MiruFilterUtils;
import com.jivesoftware.os.miru.query.MiruIntIterator;
import com.jivesoftware.os.miru.query.MiruInvertedIndex;
import com.jivesoftware.os.miru.query.MiruQueryStream;
import com.jivesoftware.os.miru.query.TermCount;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.commons.lang.mutable.MutableInt;

import static com.jivesoftware.os.miru.reco.plugins.reco.RecoResult.Recommendation;

/**
 *
 */
public class CollaborativeFiltering {

    private final MiruFilterUtils filterUtils;

    public CollaborativeFiltering(MiruFilterUtils filterUtils) {
        this.filterUtils = filterUtils;
    }

    /**
     * I have viewed these things; among others who have also viewed these things, what have they viewed that I have not?
     */
    public <BM> RecoResult collaborativeFiltering(MiruBitmaps<BM> bitmaps,
            MiruQueryStream<BM> stream,
            final RecoQuery query,
            Optional<RecoReport> report,
            BM answer)
            throws Exception {

        BM contributors = possibleContributors(bitmaps, stream, query, answer);
        BM otherContributors = bitmaps.create();
        bitmaps.andNot(otherContributors, contributors, Collections.singletonList(answer));
        // at this point we have all activity for all my viewed documents in 'contributors', and all activity not my own in 'otherContributors'.
        MinMaxPriorityQueue<TermCount> contributorHeap = rankContributors(bitmaps, query, stream, otherContributors);

        BM contributions = contributions(bitmaps, contributorHeap, stream, query);

        final List<TermCount> mostLike = new ArrayList<>(contributorHeap);
        final BloomIndex<BM> bloomIndex = new BloomIndex<>(bitmaps, Hashing.murmur3_128(), 100000, 0.01f); // TODO fix so how
        final List<BloomIndex.Mights<TermCount>> wantBits = bloomIndex.wantBits(mostLike);
        // TODO handle authz
        BM othersContributions = bitmaps.create();
        bitmaps.andNot(othersContributions, contributions, Collections.singletonList(contributors)); // remove activity for my viewed documents
        return score(bitmaps, query, othersContributions, stream, bloomIndex, wantBits);

    }

    private <BM> BM contributions(MiruBitmaps<BM> bitmaps, MinMaxPriorityQueue<TermCount> userHeap, MiruQueryStream<BM> stream, RecoQuery query)
            throws Exception {
        final MiruField<BM> lookupField2 = stream.fieldIndex.getField(stream.schema.getFieldId(query.lookupFieldNamed2));

        List<BM> toBeORed = new ArrayList<>();
        for (TermCount tc : userHeap) {
            Optional<MiruInvertedIndex<BM>> invertedIndex = lookupField2.getInvertedIndex(filterUtils.makeComposite(tc.termId, "^", query.aggregateFieldName3));
            if (invertedIndex.isPresent()) {
                toBeORed.add(invertedIndex.get().getIndex());
            }
        }
        BM r = bitmaps.create();
        bitmaps.or(r, toBeORed);
        return r;
    }

    private <BM> MinMaxPriorityQueue<TermCount> rankContributors(MiruBitmaps<BM> bitmaps,
            final RecoQuery query,
            MiruQueryStream<BM> stream,
            BM join1)
            throws Exception {
        MiruField<BM> aggregateField2 = stream.fieldIndex.getField(stream.schema.getFieldId(query.aggregateFieldName2));

        final MinMaxPriorityQueue<TermCount> userHeap = MinMaxPriorityQueue.orderedBy(new Comparator<TermCount>() {

            @Override
            public int compare(TermCount o1, TermCount o2) {
                return -Long.compare(o1.count, o2.count); // minus to reverse :)
            }
        }).maximumSize(query.resultCount).create(); // overloaded :(

        filterUtils.stream(bitmaps, query.tenantId, stream, join1, Optional.<BM>absent(), aggregateField2, query.retrieveFieldName2,
                new CallbackStream<TermCount>() {
                    @Override
                    public TermCount callback(TermCount v) throws Exception {
                        if (v != null) {
                            userHeap.add(v);
                        }
                        return v;
                    }
                });
        return userHeap;
    }

    public <BM> BM possibleContributors(MiruBitmaps<BM> bitmaps, MiruQueryStream<BM> stream, final RecoQuery query, BM answer) throws Exception {
        MiruField<BM> aggregateField1 = stream.fieldIndex.getField(stream.schema.getFieldId(query.aggregateFieldName1));
        // feeds us our docIds
        List<BM> toBeORed = new ArrayList<>();
        MiruIntIterator answerIterator = bitmaps.intIterator(answer);
        int fieldId = stream.schema.getFieldId(query.aggregateFieldName1);
        while (answerIterator.hasNext()) {
            int id = answerIterator.next();
            MiruTermId[] fieldValues = stream.activityIndex.get(query.tenantId, id, fieldId);
            if (fieldValues != null && fieldValues.length > 0) {
                Optional<MiruInvertedIndex<BM>> invertedIndex = aggregateField1.getInvertedIndex(
                        filterUtils.makeComposite(fieldValues[0], "^", query.aggregateFieldName2));
                if (invertedIndex.isPresent()) {
                    toBeORed.add(invertedIndex.get().getIndex());
                }
            }
        }
        BM r = bitmaps.create();
        bitmaps.or(r, toBeORed);
        return r;
    }

    private <BM> RecoResult score(MiruBitmaps<BM> bitmaps, final RecoQuery query, BM join2, MiruQueryStream<BM> stream,
            final BloomIndex<BM> bloomIndex, final List<BloomIndex.Mights<TermCount>> wantBits) throws Exception {

        final MiruField<BM> aggregateField3 = stream.fieldIndex.getField(stream.schema.getFieldId(query.aggregateFieldName3));

        final MinMaxPriorityQueue<TermCount> heap = MinMaxPriorityQueue.orderedBy(new Comparator<TermCount>() {

            @Override
            public int compare(TermCount o1, TermCount o2) {
                return -Long.compare(o1.count, o2.count); // minus to reverse :)
            }
        }).maximumSize(query.resultCount).create();
        // feeds us all recommended documents
        filterUtils.stream(bitmaps, query.tenantId, stream, join2, Optional.<BM>absent(), aggregateField3, query.retrieveFieldName3,
                new CallbackStream<TermCount>() {
                    @Override
                    public TermCount callback(TermCount v) throws Exception {
                        if (v != null) {
                            MiruTermId[] fieldValues = v.mostRecent;
                            if (fieldValues != null && fieldValues.length > 0) {
                                Optional<MiruInvertedIndex<BM>> invertedIndex = aggregateField3.getInvertedIndex(
                                        filterUtils.makeComposite(fieldValues[0], "|", query.retrieveFieldName2));
                                if (invertedIndex.isPresent()) {
                                    MiruInvertedIndex<BM> index = invertedIndex.get();
                                    final MutableInt count = new MutableInt(0);
                                    bloomIndex.mightContain(index, wantBits, new BloomIndex.MightContain<TermCount>() {

                                        @Override
                                        public void mightContain(TermCount value) {
                                            count.add(value.count);
                                        }
                                    });
                                    heap.add(new TermCount(fieldValues[0], null, count.longValue()));

                                    for (BloomIndex.Mights<TermCount> boo : wantBits) {
                                        boo.reset();
                                    }
                                }
                            }
                        }
                        return v;
                    }
                });

        List<Recommendation> results = new ArrayList<>();
        for (TermCount result : heap) {
            results.add(new Recommendation(result.termId, result.count));
        }
        return new RecoResult(results);
    }

}
