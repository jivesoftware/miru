package com.jivesoftware.os.miru.service.stream.factory;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Bytes;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.query.AggregateCountsQuery;
import com.jivesoftware.os.miru.api.query.DistinctCountQuery;
import com.jivesoftware.os.miru.api.query.RecoQuery;
import com.jivesoftware.os.miru.api.query.TrendingQuery;
import com.jivesoftware.os.miru.api.query.result.AggregateCountsResult;
import com.jivesoftware.os.miru.api.query.result.AggregateCountsResult.AggregateCount;
import com.jivesoftware.os.miru.api.query.result.DistinctCountResult;
import com.jivesoftware.os.miru.api.query.result.RecoResult;
import com.jivesoftware.os.miru.api.query.result.RecoResult.Recommendation;
import com.jivesoftware.os.miru.api.query.result.TrendingResult;
import com.jivesoftware.os.miru.api.query.result.TrendingResult.Trendy;
import com.jivesoftware.os.miru.reco.trending.SimpleRegressionTrend;
import com.jivesoftware.os.miru.service.activity.MiruInternalActivity;
import com.jivesoftware.os.miru.service.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.service.bitmap.MiruBitmaps.CardinalityAndLastSetBit;
import com.jivesoftware.os.miru.service.bitmap.MiruIntIterator;
import com.jivesoftware.os.miru.service.index.BloomIndex;
import com.jivesoftware.os.miru.service.index.MiruField;
import com.jivesoftware.os.miru.service.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.service.index.ReusableBuffers;
import com.jivesoftware.os.miru.service.query.AggregateCountsReport;
import com.jivesoftware.os.miru.service.query.DistinctCountReport;
import com.jivesoftware.os.miru.service.query.RecoReport;
import com.jivesoftware.os.miru.service.query.TrendingReport;
import com.jivesoftware.os.miru.service.stream.MiruActivityInternExtern;
import com.jivesoftware.os.miru.service.stream.MiruQueryStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.mutable.MutableInt;

import static com.google.common.base.Preconditions.checkState;

/**
 * @author jonathan
 */
public class MiruFilterUtils<BM> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();
    private final MiruBitmaps<BM> bitmaps;
    private final MiruActivityInternExtern activityInternExtern;

    public MiruFilterUtils(MiruBitmaps<BM> bitmaps, MiruActivityInternExtern activityInternExtern) {
        this.bitmaps = bitmaps;
        this.activityInternExtern = activityInternExtern;
    }

    public AggregateCountsResult getAggregateCounts(MiruQueryStream<BM> stream, AggregateCountsQuery query, Optional<AggregateCountsReport> lastReport,
            BM answer, Optional<BM> counter) throws Exception {

        log.debug("Get aggregate counts for answer {}", answer);

        int collectedDistincts = 0;
        int skippedDistincts = 0;
        Set<MiruTermId> aggregateTerms;
        if (lastReport.isPresent()) {
            collectedDistincts = lastReport.get().collectedDistincts;
            skippedDistincts = lastReport.get().skippedDistincts;
            aggregateTerms = Sets.newHashSet(lastReport.get().aggregateTerms);
        } else {
            aggregateTerms = Sets.newHashSet();
        }

        List<AggregateCount> aggregateCounts = new ArrayList<>();
        int fieldId = stream.schema.getFieldId(query.aggregateCountAroundField);
        if (fieldId >= 0) {
            MiruField<BM> aggregateField = stream.fieldIndex.getField(fieldId);

            BM unreadIndex = null;
            if (query.streamId.isPresent()) {
                Optional<BM> unread = stream.unreadTrackingIndex.getUnread(query.streamId.get());
                if (unread.isPresent()) {
                    unreadIndex = unread.get();
                }
            }

            // 2 to swap answers, 2 to swap counters, 1 to check unread
            final int numBuffers = 2 + (counter.isPresent() ? 2 : 0) + (unreadIndex != null ? 1 : 0);
            ReusableBuffers<BM> reusable = new ReusableBuffers<>(bitmaps, numBuffers);

            long beforeCount = counter.isPresent() ? bitmaps.cardinality(counter.get()) : bitmaps.cardinality(answer);
            CardinalityAndLastSetBit answerCollector = null;
            for (MiruTermId aggregateTermId : aggregateTerms) { // Consider
                Optional<MiruInvertedIndex<BM>> invertedIndex = aggregateField.getInvertedIndex(aggregateTermId);
                if (!invertedIndex.isPresent()) {
                    continue;
                }

                BM termIndex = invertedIndex.get().getIndex();
                BM revisedAnswer = reusable.next();
                answerCollector = bitmaps.andNotWithCardinalityAndLastSetBit(revisedAnswer, answer, termIndex);
                answer = revisedAnswer;

                long afterCount;
                if (counter.isPresent()) {
                    BM revisedCounter = reusable.next();
                    CardinalityAndLastSetBit counterCollector = bitmaps.andNotWithCardinalityAndLastSetBit(revisedCounter, counter.get(), termIndex);
                    counter = Optional.of(revisedCounter);
                    afterCount = counterCollector.cardinality;
                } else {
                    afterCount = answerCollector.cardinality;
                }

                boolean unread = false;
                if (unreadIndex != null) {
                    BM unreadAnswer = reusable.next();
                    CardinalityAndLastSetBit storage = bitmaps.andWithCardinalityAndLastSetBit(unreadAnswer, Arrays.asList(unreadIndex, termIndex));
                    if (storage.cardinality > 0) {
                        unread = true;
                    }
                }

                aggregateCounts.add(new AggregateCount(null, aggregateTermId.getBytes(), beforeCount - afterCount, unread));
                beforeCount = afterCount;
            }

            while (true) {
                int lastSetBit = answerCollector == null ? lastSetBit(answer) : answerCollector.lastSetBit;
                if (lastSetBit < 0) {
                    break;
                }

                MiruInternalActivity activity = stream.activityIndex.get(lastSetBit);
                MiruTermId[] fieldValues = activity.fieldsValues[fieldId];
                if (fieldValues == null || fieldValues.length == 0) {
                    // could make this a reusable buffer, but this is effectively an error case and would require 3 buffers
                    BM removeUnknownField = bitmaps.create();
                    bitmaps.set(removeUnknownField, lastSetBit);
                    BM revisedAnswer = reusable.next();
                    answerCollector = bitmaps.andNotWithCardinalityAndLastSetBit(revisedAnswer, answer, removeUnknownField);
                    answer = revisedAnswer;
                    beforeCount--;

                } else {
                    MiruTermId aggregateTermId = fieldValues[0]; // Kinda lame but for now we don't see a need for multi field aggregation.
                    byte[] aggregateValue = aggregateTermId.getBytes();
                    aggregateTerms.add(aggregateTermId);

                    Optional<MiruInvertedIndex<BM>> invertedIndex = aggregateField.getInvertedIndex(aggregateTermId);
                    checkState(invertedIndex.isPresent(), "Unable to load inverted index for aggregateTermId: " + aggregateTermId);

                    BM termIndex = invertedIndex.get().getIndex();

                    BM revisedAnswer = reusable.next();
                    answerCollector = bitmaps.andNotWithCardinalityAndLastSetBit(revisedAnswer, answer, termIndex);
                    answer = revisedAnswer;

                    long afterCount;
                    if (counter.isPresent()) {
                        BM revisedCounter = reusable.next();
                        CardinalityAndLastSetBit counterCollector = bitmaps.andNotWithCardinalityAndLastSetBit(revisedCounter, counter.get(), termIndex);
                        counter = Optional.of(revisedCounter);
                        afterCount = counterCollector.cardinality;
                    } else {
                        afterCount = answerCollector.cardinality;
                    }

                    collectedDistincts++;
                    if (collectedDistincts > query.startFromDistinctN) {
                        boolean unread = false;
                        if (unreadIndex != null) {
                            BM unreadAnswer = reusable.next();
                            CardinalityAndLastSetBit storage = bitmaps.andNotWithCardinalityAndLastSetBit(unreadAnswer, unreadIndex, termIndex);
                            if (storage.cardinality > 0) {
                                unread = true;
                            }
                        }

                        AggregateCount aggregateCount = new AggregateCount(activityInternExtern.extern(activity), aggregateValue, beforeCount - afterCount, unread);
                        aggregateCounts.add(aggregateCount);

                        if (aggregateCounts.size() >= query.desiredNumberOfDistincts) {
                            break;
                        }
                    } else {
                        skippedDistincts++;
                    }
                    beforeCount = afterCount;
                }
            }
        }

        return new AggregateCountsResult(ImmutableList.copyOf(aggregateCounts), ImmutableSet.copyOf(aggregateTerms), skippedDistincts, collectedDistincts);
    }

    public DistinctCountResult numberOfDistincts(MiruQueryStream<BM> stream, DistinctCountQuery query, Optional<DistinctCountReport> lastReport,
            BM answer) throws Exception {

        log.debug("Get number of distincts for answer {}", answer);

        int collectedDistincts = 0;
        Set<MiruTermId> aggregateTerms;
        if (lastReport.isPresent()) {
            collectedDistincts = lastReport.get().collectedDistincts;
            aggregateTerms = Sets.newHashSet(lastReport.get().aggregateTerms);
        } else {
            aggregateTerms = Sets.newHashSet();
        }

        int fieldId = stream.schema.getFieldId(query.aggregateCountAroundField);
        if (fieldId >= 0) {
            MiruField<BM> aggregateField = stream.fieldIndex.getField(fieldId);
            ReusableBuffers<BM> reusable = new ReusableBuffers<>(bitmaps, 2);

            for (MiruTermId aggregateTermId : aggregateTerms) {
                Optional<MiruInvertedIndex<BM>> invertedIndex = aggregateField.getInvertedIndex(aggregateTermId);
                if (!invertedIndex.isPresent()) {
                    continue;
                }

                BM termIndex = invertedIndex.get().getIndex();

                BM revisedAnswer = reusable.next();
                bitmaps.andNot(revisedAnswer, answer, Collections.<BM>singletonList(termIndex));
                answer = revisedAnswer;
            }

            CardinalityAndLastSetBit answerCollector = null;
            while (true) {
                int lastSetBit = answerCollector == null ? lastSetBit(answer) : answerCollector.lastSetBit;
                if (lastSetBit < 0) {
                    break;
                }
                MiruInternalActivity activity = stream.activityIndex.get(lastSetBit);

                MiruTermId[] fieldValues = activity.fieldsValues[fieldId];
                if (fieldValues == null || fieldValues.length == 0) {
                    // could make this a reusable buffer, but this is effectively an error case and would require 3 buffers
                    BM removeUnknownField = bitmaps.create();
                    bitmaps.set(removeUnknownField, lastSetBit);
                    BM revisedAnswer = reusable.next();
                    answerCollector = bitmaps.andNotWithCardinalityAndLastSetBit(revisedAnswer, answer, removeUnknownField);
                    answer = revisedAnswer;

                } else {
                    MiruTermId aggregateTermId = fieldValues[0];

                    aggregateTerms.add(aggregateTermId);
                    Optional<MiruInvertedIndex<BM>> invertedIndex = aggregateField.getInvertedIndex(aggregateTermId);
                    checkState(invertedIndex.isPresent(), "Unable to load inverted index for aggregateTermId: " + aggregateTermId);

                    BM revisedAnswer = reusable.next();
                    answerCollector = bitmaps.andNotWithCardinalityAndLastSetBit(revisedAnswer, answer, invertedIndex.get().getIndex());
                    answer = revisedAnswer;

                    collectedDistincts++;

                    if (collectedDistincts > query.desiredNumberOfDistincts) {
                        break;
                    }
                }
            }
        }
        return new DistinctCountResult(ImmutableSet.copyOf(aggregateTerms), collectedDistincts);
    }

    public TrendingResult trending(MiruQueryStream<BM> stream, TrendingQuery query, Optional<TrendingReport> lastReport, BM answer)
            throws Exception {

        log.debug("Get trending for answer {}", answer);

        int collectedDistincts = 0;
        Set<MiruTermId> aggregateTerms;
        if (lastReport.isPresent()) {
            collectedDistincts = lastReport.get().collectedDistincts;
            aggregateTerms = Sets.newHashSet(lastReport.get().aggregateTerms);
        } else {
            aggregateTerms = Sets.newHashSet();
        }

        List<Trendy> trendies = new ArrayList<>();
        int fieldId = stream.schema.getFieldId(query.aggregateCountAroundField);
        if (fieldId >= 0) {
            MiruField<BM> aggregateField = stream.fieldIndex.getField(fieldId);

            ReusableBuffers<BM> reusable = new ReusableBuffers<>(bitmaps, 2);
            for (MiruTermId aggregateTermId : aggregateTerms) { // Consider
                Optional<MiruInvertedIndex<BM>> invertedIndex = aggregateField.getInvertedIndex(aggregateTermId);
                if (!invertedIndex.isPresent()) {
                    continue;
                }

                BM termIndex = invertedIndex.get().getIndex();
                BM revisedAnswer = reusable.next();
                bitmaps.andNot(revisedAnswer, answer, Collections.<BM>singletonList(termIndex));
                answer = revisedAnswer;

                SimpleRegressionTrend trend = new SimpleRegressionTrend();
                MiruIntIterator iter = bitmaps.intIterator(termIndex);
                while (iter.hasNext()) {
                    int index = iter.next();
                    long timestamp = stream.timeIndex.getTimestamp(index);
                    trend.add(timestamp, 1d);
                }
                trendies.add(new Trendy(null, aggregateTermId.getBytes(), trend, trend.getRank(trend.getCurrentT())));
            }

            CardinalityAndLastSetBit answerCollector = null;
            while (true) {
                int lastSetBit = answerCollector == null ? lastSetBit(answer) : answerCollector.lastSetBit;
                if (lastSetBit < 0) {
                    break;
                }

                MiruInternalActivity activity = stream.activityIndex.get(lastSetBit);
                MiruTermId[] fieldValues = activity.fieldsValues[fieldId];
                if (fieldValues == null || fieldValues.length == 0) {
                    // could make this a reusable buffer, but this is effectively an error case and would require 3 buffers
                    BM removeUnknownField = bitmaps.create();
                    bitmaps.set(removeUnknownField, lastSetBit);
                    BM revisedAnswer = reusable.next();
                    answerCollector = bitmaps.andNotWithCardinalityAndLastSetBit(revisedAnswer, answer, removeUnknownField);
                    answer = revisedAnswer;

                } else {
                    MiruTermId aggregateTermId = fieldValues[0]; // Kinda lame but for now we don't see a need for multi field aggregation.
                    byte[] aggregateValue = aggregateTermId.getBytes();
                    aggregateTerms.add(aggregateTermId);

                    Optional<MiruInvertedIndex<BM>> invertedIndex = aggregateField.getInvertedIndex(aggregateTermId);
                    checkState(invertedIndex.isPresent(), "Unable to load inverted index for aggregateTermId: " + aggregateTermId);

                    BM termIndex = invertedIndex.get().getIndex();

                    BM revisedAnswer = reusable.next();
                    answerCollector = bitmaps.andNotWithCardinalityAndLastSetBit(revisedAnswer, answer, termIndex);
                    answer = revisedAnswer;

                    collectedDistincts++;

                    SimpleRegressionTrend trend = new SimpleRegressionTrend();
                    MiruIntIterator iter = bitmaps.intIterator(termIndex);
                    while (iter.hasNext()) {
                        int index = iter.next();
                        long timestamp = stream.timeIndex.getTimestamp(index);
                        trend.add(timestamp, 1d);
                    }
                    Trendy trendy = new Trendy(activityInternExtern.extern(activity), aggregateValue, trend, trend.getRank(trend.getCurrentT()));
                    trendies.add(trendy);

                    //TODO desiredNumberOfDistincts is used to truncate the final list. Do we need a maxDistincts of some sort?
                    /*
                     if (trendies.size() >= query.desiredNumberOfDistincts) {
                     break;
                     }
                     */
                }
            }
        }

        return new TrendingResult(ImmutableList.copyOf(trendies), ImmutableSet.copyOf(aggregateTerms), collectedDistincts);
    }

    public BM bufferedAnd(List<BM> ands) {
        if (ands.isEmpty()) {
            return bitmaps.create();
        } else if (ands.size() == 1) {
            return ands.get(0);
        } else {
            BM r = bitmaps.create();
            bitmaps.and(r, ands);
            return r;
        }
    }

    /** I have viewed these things; among others who have also viewed these things, what have they viewed that I have not? */
    RecoResult collaborativeFiltering(MiruQueryStream<BM> stream, final RecoQuery query, Optional<RecoReport> report, BM answer) throws Exception {

        BM contributors = possibleContributors(stream, query, answer);
        BM otherContributors = bitmaps.create();
        bitmaps.andNot(otherContributors, contributors, Collections.singletonList(answer));
        // at this point we have all activity for all my viewed documents in 'contributors', and all activity not my own in 'otherContributors'.
        MinMaxPriorityQueue<TermCount> contributorHeap = rankContributors(query, stream, otherContributors);

        BM contributions = contributions(contributorHeap, stream, query);

        final List<TermCount> mostLike = new ArrayList<>(contributorHeap);
        final BloomIndex<BM> bloomIndex = new BloomIndex<>(bitmaps, Hashing.murmur3_128(), 100000, 0.01f); // TODO fix so how
        final List<BloomIndex.Mights<TermCount>> wantBits = bloomIndex.wantBits(mostLike);
        // TODO handle authz
        BM othersContributions = bitmaps.create();
        bitmaps.andNot(othersContributions, contributions, Collections.singletonList(contributors)); // remove activity for my viewed documents
        return score(query, othersContributions, stream, bloomIndex, wantBits);

    }

    private BM contributions(MinMaxPriorityQueue<TermCount> userHeap, MiruQueryStream<BM> stream, RecoQuery query) throws Exception {
        final MiruField<BM> lookupField2 = stream.fieldIndex.getField(stream.schema.getFieldId(query.lookupFieldNamed2));

        List<BM> toBeORed = new ArrayList<>();
        for (TermCount tc : userHeap) {
            Optional<MiruInvertedIndex<BM>> invertedIndex = lookupField2.getInvertedIndex(makeComposite(tc.termId, "^", query.aggregateFieldName3));
            if (invertedIndex.isPresent()) {
                toBeORed.add(invertedIndex.get().getIndex());
            }
        }
        BM r = bitmaps.create();
        bitmaps.or(r, toBeORed);
        return r;
    }

    private MinMaxPriorityQueue<TermCount> rankContributors(final RecoQuery query, MiruQueryStream<BM> stream, BM join1) throws Exception {
        MiruField<BM> aggregateField2 = stream.fieldIndex.getField(stream.schema.getFieldId(query.aggregateFieldName2));

        final MinMaxPriorityQueue<TermCount> userHeap = MinMaxPriorityQueue.orderedBy(new Comparator<TermCount>() {

            @Override
            public int compare(TermCount o1, TermCount o2) {
                return -Long.compare(o1.count, o2.count); // minus to reverse :)
            }
        }).maximumSize(query.resultCount).create(); // overloaded :(
        stream(stream, join1, Optional.<BM>absent(), aggregateField2, query.retrieveFieldName2, new CallbackStream<TermCount>() {

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

    public BM possibleContributors(MiruQueryStream<BM> stream, final RecoQuery query, BM answer) throws Exception {
        MiruField<BM> aggregateField1 = stream.fieldIndex.getField(stream.schema.getFieldId(query.aggregateFieldName1));
        // feeds us our docIds
        List<BM> toBeORed = new ArrayList<>();
        MiruIntIterator answerIterator = bitmaps.intIterator(answer);
        int fieldId = stream.schema.getFieldId(query.aggregateFieldName1);
        while (answerIterator.hasNext()) {
            int id = answerIterator.next();
            MiruInternalActivity activity = stream.activityIndex.get(id);
            MiruTermId[] fieldValues = activity.fieldsValues[fieldId];
            if (fieldValues != null && fieldValues.length > 0) {
                Optional<MiruInvertedIndex<BM>> invertedIndex = aggregateField1.getInvertedIndex(makeComposite(fieldValues[0], "^", query.aggregateFieldName2));
                if (invertedIndex.isPresent()) {
                    toBeORed.add(invertedIndex.get().getIndex());
                }
            }
        }
        BM r = bitmaps.create();
        bitmaps.or(r, toBeORed);
        return r;
    }

    private RecoResult score(final RecoQuery query, BM join2, MiruQueryStream<BM> stream,
            final BloomIndex<BM> bloomIndex, final List<BloomIndex.Mights<TermCount>> wantBits) throws Exception {

        final MiruField<BM> aggregateField3 = stream.fieldIndex.getField(stream.schema.getFieldId(query.aggregateFieldName3));

        final MinMaxPriorityQueue<TermCount> heap = MinMaxPriorityQueue.orderedBy(new Comparator<TermCount>() {

            @Override
            public int compare(TermCount o1, TermCount o2) {
                return -Long.compare(o1.count, o2.count); // minus to reverse :)
            }
        }).maximumSize(query.resultCount).create();
        // feeds us all recommended documents
        final int fieldId = stream.schema.getFieldId(query.retrieveFieldName3);
        stream(stream, join2, Optional.<BM>absent(), aggregateField3, query.retrieveFieldName3, new CallbackStream<TermCount>() {

            @Override
            public TermCount callback(TermCount v) throws Exception {
                if (v != null) {
                    MiruTermId[] fieldValues = v.mostRecent.fieldsValues[fieldId];
                    if (fieldValues != null && fieldValues.length > 0) {
                        Optional<MiruInvertedIndex<BM>> invertedIndex = aggregateField3.getInvertedIndex(makeComposite(fieldValues[0], "|", query.retrieveFieldName2));
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

    private MiruTermId makeComposite(MiruTermId fieldValue, String separator, String fieldName) {
        return new MiruTermId(Bytes.concat(fieldValue.getBytes(), separator.getBytes(), fieldName.getBytes()));
    }

    private void stream(MiruQueryStream stream, BM answer,
            Optional<BM> counter,
            MiruField<BM> pivotField, String streamField, CallbackStream<TermCount> terms) throws Exception {

        final AtomicLong bytesTraversed = new AtomicLong();
        bytesTraversed.addAndGet(bitmaps.sizeInBytes(answer));
        CardinalityAndLastSetBit answerCollector = null;
        ReusableBuffers<BM> reusable = new ReusableBuffers<>(bitmaps, 2);
        int fieldId = stream.schema.getFieldId(streamField);
        long beforeCount = counter.isPresent() ? bitmaps.cardinality(counter.get()) : bitmaps.cardinality(answer);
        while (true) {
            int lastSetBit = answerCollector == null ? lastSetBit(answer) : answerCollector.lastSetBit;
            if (lastSetBit < 0) {
                break;
            }

            MiruInternalActivity activity = stream.activityIndex.get(lastSetBit);
            MiruTermId[] fieldValues = activity.fieldsValues[fieldId];
            if (fieldValues == null || fieldValues.length == 0) {
                // could make this a reusable buffer, but this is effectively an error case and would require 3 buffers
                BM removeUnknownField = bitmaps.create();
                bitmaps.set(removeUnknownField, lastSetBit);
                bytesTraversed.addAndGet(Math.max(bitmaps.sizeInBytes(answer), bitmaps.sizeInBytes(removeUnknownField)));

                BM revisedAnswer = reusable.next();
                answerCollector = bitmaps.andNotWithCardinalityAndLastSetBit(revisedAnswer, answer, removeUnknownField);
                answer = revisedAnswer;

            } else {
                MiruTermId pivotTerm = fieldValues[0]; // Kinda lame but for now we don't see a need for multi field aggregation.

                Optional<MiruInvertedIndex<BM>> invertedIndex = pivotField.getInvertedIndex(pivotTerm);
                checkState(invertedIndex.isPresent(), "Unable to load inverted index for aggregateTermId: " + pivotTerm);

                BM termIndex = invertedIndex.get().getIndex();
                bytesTraversed.addAndGet(Math.max(bitmaps.sizeInBytes(answer), bitmaps.sizeInBytes(termIndex)));

                BM revisedAnswer = reusable.next();
                answerCollector = bitmaps.andNotWithCardinalityAndLastSetBit(revisedAnswer, answer, termIndex);
                answer = revisedAnswer;

                long afterCount;
                if (counter.isPresent()) {
                    BM revisedCounter = reusable.next();
                    CardinalityAndLastSetBit counterCollector = bitmaps.andNotWithCardinalityAndLastSetBit(revisedCounter, counter.get(), termIndex);
                    counter = Optional.of(revisedCounter);
                    afterCount = counterCollector.cardinality;
                } else {
                    afterCount = answerCollector.cardinality;
                }

                TermCount termCount = new TermCount(pivotTerm, activity, beforeCount - afterCount);
                if (termCount != terms.callback(termCount)) { // Stop stream
                    return;
                }
                beforeCount = afterCount;
            }

        }
        terms.callback(null); // EOS
        System.out.println("Bytes Traversed=" + bytesTraversed.longValue());

    }

    public int lastSetBit(BM bitmap) {
        MiruIntIterator iterator = bitmaps.intIterator(bitmap);
        int last = -1;
        for (; iterator.hasNext();) {
            last = iterator.next();
        }
        return last;
    }

    static class TermCount implements BloomIndex.HasValue {

        public final MiruTermId termId;
        public MiruInternalActivity mostRecent;
        public final long count;

        public TermCount(MiruTermId termId, MiruInternalActivity mostRecent, long count) {
            this.termId = termId;
            this.mostRecent = mostRecent;
            this.count = count;
        }

        @Override
        public byte[] getValue() {
            return termId.getBytes();
        }

        @Override
        public String toString() {
            return "TermCount{" + "termId=" + termId + ", mostRecent=" + mostRecent + ", count=" + count + '}';
        }

    }

}
