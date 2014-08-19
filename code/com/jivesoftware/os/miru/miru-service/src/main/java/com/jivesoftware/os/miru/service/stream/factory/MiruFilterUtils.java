package com.jivesoftware.os.miru.service.stream.factory;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Bytes;
import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.googlecode.javaewah.FastAggregation;
import com.googlecode.javaewah.IntIterator;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
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
import com.jivesoftware.os.miru.service.index.*;
import com.jivesoftware.os.miru.service.query.AggregateCountsReport;
import com.jivesoftware.os.miru.service.query.DistinctCountReport;
import com.jivesoftware.os.miru.service.query.RecoReport;
import com.jivesoftware.os.miru.service.query.TrendingReport;
import com.jivesoftware.os.miru.service.stream.MiruQueryStream;
import com.jivesoftware.os.miru.service.stream.filter.AnswerCardinalityLastSetBitmapStorage;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.commons.lang3.mutable.MutableObject;

import static com.google.common.base.Preconditions.checkState;

/**
 * @author jonathan
 */
public class MiruFilterUtils {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    public AggregateCountsResult getAggregateCounts(MiruQueryStream stream, AggregateCountsQuery query, Optional<AggregateCountsReport> lastReport,
            EWAHCompressedBitmap answer, Optional<EWAHCompressedBitmap> counter) throws Exception {

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
            MiruField aggregateField = stream.fieldIndex.getField(fieldId);

            EWAHCompressedBitmap unreadIndex = null;
            if (query.streamId.isPresent()) {
                Optional<EWAHCompressedBitmap> unread = stream.unreadTrackingIndex.getUnread(query.streamId.get());
                if (unread.isPresent()) {
                    unreadIndex = unread.get();
                }
            }

            // 2 to swap answers, 2 to swap counters, 1 to check unread
            final int numBuffers = 2 + (counter.isPresent() ? 2 : 0) + (unreadIndex != null ? 1 : 0);
            ReusableBuffers reusable = new ReusableBuffers(numBuffers);

            int beforeCount = counter.isPresent() ? counter.get().cardinality() : answer.cardinality();
            AnswerCardinalityLastSetBitmapStorage answerCollector = null;
            for (MiruTermId aggregateTermId : aggregateTerms) { // Consider
                Optional<MiruInvertedIndex> invertedIndex = aggregateField.getInvertedIndex(aggregateTermId);
                if (!invertedIndex.isPresent()) {
                    continue;
                }

                EWAHCompressedBitmap termIndex = invertedIndex.get().getIndex();
                EWAHCompressedBitmap revisedAnswer = reusable.next();
                answerCollector = new AnswerCardinalityLastSetBitmapStorage(revisedAnswer);
                answer.andNotToContainer(termIndex, answerCollector);
                answer = revisedAnswer;

                int afterCount;
                if (counter.isPresent()) {
                    EWAHCompressedBitmap revisedCounter = reusable.next();
                    AnswerCardinalityLastSetBitmapStorage counterCollector = new AnswerCardinalityLastSetBitmapStorage(revisedCounter);
                    counter.get().andNotToContainer(termIndex, counterCollector);
                    counter = Optional.of(revisedCounter);
                    afterCount = counterCollector.getCount();
                } else {
                    afterCount = answerCollector.getCount();
                }

                boolean unread = false;
                if (unreadIndex != null) {
                    EWAHCompressedBitmap unreadAnswer = reusable.next();
                    AnswerCardinalityLastSetBitmapStorage storage = new AnswerCardinalityLastSetBitmapStorage(unreadAnswer);
                    unreadIndex.andToContainer(termIndex, storage);
                    if (storage.getCount() > 0) {
                        unread = true;
                    }
                }

                aggregateCounts.add(new AggregateCount(null, aggregateTermId.getBytes(), beforeCount - afterCount, unread));
                beforeCount = afterCount;
            }

            while (true) {
                int lastSetBit = answerCollector == null ? lastSetBit(answer) : answerCollector.getLastSetBit();
                if (lastSetBit < 0) {
                    break;
                }

                MiruActivity activity = stream.activityIndex.get(lastSetBit);
                MiruTermId[] fieldValues = activity.fieldsValues.get(query.aggregateCountAroundField);
                if (fieldValues == null || fieldValues.length == 0) {
                    // could make this a reusable buffer, but this is effectively an error case and would require 3 buffers
                    EWAHCompressedBitmap removeUnknownField = new EWAHCompressedBitmap();
                    removeUnknownField.set(lastSetBit);
                    EWAHCompressedBitmap revisedAnswer = reusable.next();
                    answerCollector = new AnswerCardinalityLastSetBitmapStorage(revisedAnswer);
                    answer.andNotToContainer(removeUnknownField, answerCollector);
                    answer = revisedAnswer;
                    beforeCount--;

                } else {
                    MiruTermId aggregateTermId = fieldValues[0]; // Kinda lame but for now we don't see a need for multi field aggregation.
                    byte[] aggregateValue = aggregateTermId.getBytes();
                    aggregateTerms.add(aggregateTermId);

                    Optional<MiruInvertedIndex> invertedIndex = aggregateField.getInvertedIndex(aggregateTermId);
                    checkState(invertedIndex.isPresent(), "Unable to load inverted index for aggregateTermId: " + aggregateTermId);

                    EWAHCompressedBitmap termIndex = invertedIndex.get().getIndex();

                    EWAHCompressedBitmap revisedAnswer = reusable.next();
                    answerCollector = new AnswerCardinalityLastSetBitmapStorage(revisedAnswer);
                    answer.andNotToContainer(termIndex, answerCollector);
                    answer = revisedAnswer;

                    int afterCount;
                    if (counter.isPresent()) {
                        EWAHCompressedBitmap revisedCounter = reusable.next();
                        AnswerCardinalityLastSetBitmapStorage counterCollector = new AnswerCardinalityLastSetBitmapStorage(revisedCounter);
                        counter.get().andNotToContainer(termIndex, counterCollector);
                        counter = Optional.of(revisedCounter);
                        afterCount = counterCollector.getCount();
                    } else {
                        afterCount = answerCollector.getCount();
                    }

                    collectedDistincts++;
                    if (collectedDistincts > query.startFromDistinctN) {
                        boolean unread = false;
                        if (unreadIndex != null) {
                            EWAHCompressedBitmap unreadAnswer = reusable.next();
                            AnswerCardinalityLastSetBitmapStorage storage = new AnswerCardinalityLastSetBitmapStorage(unreadAnswer);
                            unreadIndex.andToContainer(termIndex, storage);
                            if (storage.getCount() > 0) {
                                unread = true;
                            }
                        }

                        AggregateCount aggregateCount = new AggregateCount(activity, aggregateValue, beforeCount - afterCount, unread);
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

    public DistinctCountResult numberOfDistincts(MiruQueryStream stream, DistinctCountQuery query, Optional<DistinctCountReport> lastReport,
            EWAHCompressedBitmap answer) throws Exception {

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
            MiruField aggregateField = stream.fieldIndex.getField(fieldId);
            ReusableBuffers reusable = new ReusableBuffers(2);

            for (MiruTermId aggregateTermId : aggregateTerms) {
                Optional<MiruInvertedIndex> invertedIndex = aggregateField.getInvertedIndex(aggregateTermId);
                if (!invertedIndex.isPresent()) {
                    continue;
                }

                EWAHCompressedBitmap termIndex = invertedIndex.get().getIndex();

                EWAHCompressedBitmap revisedAnswer = reusable.next();
                answer.andNotToContainer(termIndex, revisedAnswer);
                answer = revisedAnswer;
            }

            AnswerCardinalityLastSetBitmapStorage answerCollector = null;
            while (true) {
                int lastSetBit = answerCollector == null ? lastSetBit(answer) : answerCollector.getLastSetBit();
                if (lastSetBit < 0) {
                    break;
                }
                MiruActivity activity = stream.activityIndex.get(lastSetBit);

                MiruTermId[] fieldValues = activity.fieldsValues.get(query.aggregateCountAroundField);
                if (fieldValues == null || fieldValues.length == 0) {
                    // could make this a reusable buffer, but this is effectively an error case and would require 3 buffers
                    EWAHCompressedBitmap removeUnknownField = new EWAHCompressedBitmap();
                    removeUnknownField.set(lastSetBit);
                    EWAHCompressedBitmap revisedAnswer = reusable.next();
                    answerCollector = new AnswerCardinalityLastSetBitmapStorage(revisedAnswer);
                    answer.andNotToContainer(removeUnknownField, answerCollector);
                    answer = revisedAnswer;

                } else {
                    MiruTermId aggregateTermId = fieldValues[0];

                    aggregateTerms.add(aggregateTermId);
                    Optional<MiruInvertedIndex> invertedIndex = aggregateField.getInvertedIndex(aggregateTermId);
                    checkState(invertedIndex.isPresent(), "Unable to load inverted index for aggregateTermId: " + aggregateTermId);

                    EWAHCompressedBitmap revisedAnswer = reusable.next();
                    answerCollector = new AnswerCardinalityLastSetBitmapStorage(revisedAnswer);
                    answer.andNotToContainer(invertedIndex.get().getIndex(), answerCollector);
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

    public TrendingResult trending(MiruQueryStream stream, TrendingQuery query, Optional<TrendingReport> lastReport, EWAHCompressedBitmap answer)
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
            MiruField aggregateField = stream.fieldIndex.getField(fieldId);

            ReusableBuffers reusable = new ReusableBuffers(2);
            for (MiruTermId aggregateTermId : aggregateTerms) { // Consider
                Optional<MiruInvertedIndex> invertedIndex = aggregateField.getInvertedIndex(aggregateTermId);
                if (!invertedIndex.isPresent()) {
                    continue;
                }

                EWAHCompressedBitmap termIndex = invertedIndex.get().getIndex();
                EWAHCompressedBitmap revisedAnswer = reusable.next();
                answer.andNotToContainer(termIndex, revisedAnswer);
                answer = revisedAnswer;

                SimpleRegressionTrend trend = new SimpleRegressionTrend();
                IntIterator iter = termIndex.intIterator();
                while (iter.hasNext()) {
                    int index = iter.next();
                    long timestamp = stream.timeIndex.getTimestamp(index);
                    trend.add(timestamp, 1d);
                }
                trendies.add(new Trendy(null, aggregateTermId.getBytes(), trend, trend.getRank(trend.getCurrentT())));
            }

            AnswerCardinalityLastSetBitmapStorage answerCollector = null;
            while (true) {
                int lastSetBit = answerCollector == null ? lastSetBit(answer) : answerCollector.getLastSetBit();
                if (lastSetBit < 0) {
                    break;
                }

                MiruActivity activity = stream.activityIndex.get(lastSetBit);
                MiruTermId[] fieldValues = activity.fieldsValues.get(query.aggregateCountAroundField);
                if (fieldValues == null || fieldValues.length == 0) {
                    // could make this a reusable buffer, but this is effectively an error case and would require 3 buffers
                    EWAHCompressedBitmap removeUnknownField = new EWAHCompressedBitmap();
                    removeUnknownField.set(lastSetBit);
                    EWAHCompressedBitmap revisedAnswer = reusable.next();
                    answerCollector = new AnswerCardinalityLastSetBitmapStorage(revisedAnswer);
                    answer.andNotToContainer(removeUnknownField, answerCollector);
                    answer = revisedAnswer;

                } else {
                    MiruTermId aggregateTermId = fieldValues[0]; // Kinda lame but for now we don't see a need for multi field aggregation.
                    byte[] aggregateValue = aggregateTermId.getBytes();
                    aggregateTerms.add(aggregateTermId);

                    Optional<MiruInvertedIndex> invertedIndex = aggregateField.getInvertedIndex(aggregateTermId);
                    checkState(invertedIndex.isPresent(), "Unable to load inverted index for aggregateTermId: " + aggregateTermId);

                    EWAHCompressedBitmap termIndex = invertedIndex.get().getIndex();

                    EWAHCompressedBitmap revisedAnswer = reusable.next();
                    answerCollector = new AnswerCardinalityLastSetBitmapStorage(revisedAnswer);
                    answer.andNotToContainer(termIndex, answerCollector);
                    answer = revisedAnswer;

                    collectedDistincts++;

                    SimpleRegressionTrend trend = new SimpleRegressionTrend();
                    IntIterator iter = termIndex.intIterator();
                    while (iter.hasNext()) {
                        int index = iter.next();
                        long timestamp = stream.timeIndex.getTimestamp(index);
                        trend.add(timestamp, 1d);
                    }
                    Trendy trendy = new Trendy(activity, aggregateValue, trend, trend.getRank(trend.getCurrentT()));
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

    public EWAHCompressedBitmap bufferedAnd(List<EWAHCompressedBitmap> ands, int bitsetBufferSize) {
        if (ands.isEmpty()) {
            return new EWAHCompressedBitmap();
        } else if (ands.size() == 1) {
            return ands.get(0);
        } else {
            return FastAggregation.bufferedand(bitsetBufferSize, ands.toArray(new EWAHCompressedBitmap[ands.size()]));
        }
    }

    public EWAHCompressedBitmap buildTimeRangeMask(MiruTimeIndex timeIndex, long smallestTimestamp, long largestTimestamp) {
        int smallestId = timeIndex.smallestExclusiveTimestampIndex(smallestTimestamp);
        int largestId = timeIndex.largestInclusiveTimestampIndex(largestTimestamp);

        EWAHCompressedBitmap mask = new EWAHCompressedBitmap();

        if (largestId < 0 || smallestId > largestId) {
            return mask;
        }

        int initialZeroWords = smallestId / EWAHCompressedBitmap.wordinbits;
        if (initialZeroWords > 0) {
            mask.addStreamOfEmptyWords(false, initialZeroWords);
        }

        //TODO[LP] see if there's a way to simplify this logic
        if (largestId == smallestId) {
            // one bit to set
            mask.set(smallestId);
        } else if (largestId < (smallestId - smallestId % EWAHCompressedBitmap.wordinbits + EWAHCompressedBitmap.wordinbits)) {
            // start and stop in same word
            int firstOne = smallestId - initialZeroWords * EWAHCompressedBitmap.wordinbits;
            int numberOfOnes = largestId - smallestId + 1;
            long word = 0;
            for (int i = 0; i < numberOfOnes; i++) {
                word |= (1l << (firstOne + i));
            }
            mask.addWord(word);
        } else if (largestId > smallestId) {
            // start word, run of ones, stop word
            int onesInStartWord = EWAHCompressedBitmap.wordinbits - smallestId % EWAHCompressedBitmap.wordinbits;
            if (onesInStartWord == EWAHCompressedBitmap.wordinbits) {
                onesInStartWord = 0;
            }
            if (onesInStartWord > 0) {
                long startWord = 0;
                for (int i = 0; i < onesInStartWord; i++) {
                    startWord |= (1l << (63 - i));
                }
                mask.addWord(startWord);
            }

            int middleOneWords = (largestId - smallestId - onesInStartWord) / EWAHCompressedBitmap.wordinbits;
            if (middleOneWords > 0) {
                mask.addStreamOfEmptyWords(true, middleOneWords);
            }

            int bitsInStopWord = largestId - smallestId + 1 - onesInStartWord - middleOneWords * EWAHCompressedBitmap.wordinbits;
            if (bitsInStopWord > 0) {
                long stopWord = 0;
                for (int i = 0; i < bitsInStopWord; i++) {
                    stopWord |= (1l << i);
                }
                mask.addWord(stopWord);
            }
        }

        return mask;
    }

    public EWAHCompressedBitmap buildIndexMask(int largestIndex, Optional<EWAHCompressedBitmap> andNotMask) {
        EWAHCompressedBitmap mask = new EWAHCompressedBitmap();
        if (largestIndex < 0) {
            return mask;
        }

        int words = largestIndex / EWAHCompressedBitmap.wordinbits;
        if (words > 0) {
            mask.addStreamOfEmptyWords(true, words);
        }

        int remainingBits = largestIndex % EWAHCompressedBitmap.wordinbits + 1;
        long lastWord = 0;
        for (int i = 0; i < remainingBits; i++) {
            lastWord |= (1l << i);
        }

        // include bitsThatMatter so we don't push our pointer to the next word alignment
        mask.addWord(lastWord, remainingBits);

        if (andNotMask.isPresent() && mask.sizeInBits() > 0 && andNotMask.get().sizeInBits() > 0) {
            return mask.andNot(andNotMask.get());
        } else {
            return mask;
        }
    }

    // ~~concept~~
    //
    // d1-3, u1-3
    //
    // [u1->D] u1 -> [ 0, 0, 0, 1(d1), 0, 0, 1(d2), 0, 0, 1(d3) ] -> d1, d2, d3 (1,000 documents)
    // [u2->D] u2 -> [ 0, 1(d1), 0, 1(d4), 0, 1(d5), 1(d2), 0, 0, 0 ] -> d1, d2, d4, d5
    // [u3->D] u3 -> [ 0, 0, 0, 0, 1(d6), 0, 0, 1(d1), 1(d7) ] -> d1, d6, d7
    //
    // [d1->U] d1 -> [ 0, 0, 0, 1(u1), 0, 1(u2), 0, 0, 1(u3), 0 ] -> u1, u2, u3
    // [d2->U] d2 -> [ 0, 1(u2), 0, 0, 0, 0, 1(u1), 0, 0, 0 ] -> u1, u2
    // [d3->U] d3 -> [ 0, 0, 0, 0, 0, 0, 0, 0, 0, 1(u1) ] -> u1
    // ...d1000
    // [d*->U]: FastAggregation(r1, r2, r3...) -> [ 0, 1, 1, 0, 0, 1, 0, 1, 0 ]
    //
    // u2..u3
    //
    // [u2->D] & [d*->U] = [ 0, 1, 0, 0, 0, 0, 1, 0, 0, 0 ] -> d1, d2 = cardinality 2
    // [u3->D] & [d*->U] = [ 0, 0, 0, 1, 0, 0, 0, 0, 0, 0 ] -> d1 = cardinality 1
    // [u2, u3...] -> heap
    //
    // heap:
    // [u2->D] &~ [d*->U] = [ 0, 0, 0, 1, 0, 1, 0, 0, 0, 1 ] -> d4, d5 -> d4:2, d5:2, d8:2
    // [u3->D] &~ [d*->U] = [ 0, 1, 0, 0, 0, 0, 0, 1, 0, 0 ] -> d6, d7 -> d6:1, d7:1, d8:1
    // d4:2, d5:2, d6:1, d7:1, d8:3

    /*  I have viewd these thing who has also view these things and what other thing have the viewed that I have not.*/
    RecoResult collaborativeFiltering(MiruQueryStream stream, final RecoQuery query, Optional<RecoReport> report, EWAHCompressedBitmap answer, int bitsetBufferSize) throws Exception {
        return collaborativeFilteringWithStreams(stream, query, report, answer, bitsetBufferSize);
    }

    RecoResult collaborativeFilteringWithStreams(MiruQueryStream stream, final RecoQuery query, Optional<RecoReport> report, EWAHCompressedBitmap answer, int bitsetBufferSize) throws Exception {

        MiruField aggregateField1 = stream.fieldIndex.getField(stream.schema.getFieldId(query.aggregateFieldName1));
        final MiruField lookupField1 = stream.fieldIndex.getField(stream.schema.getFieldId(query.lookupFieldNamed1));

        MiruField aggregateField2 = stream.fieldIndex.getField(stream.schema.getFieldId(query.aggregateFieldName2));
        final MiruField lookupField2 = stream.fieldIndex.getField(stream.schema.getFieldId(query.lookupFieldNamed2));

        final MiruField aggregateField3 = stream.fieldIndex.getField(stream.schema.getFieldId(query.aggregateFieldName3));

        // feeds us our docIds
        final MutableObject<EWAHCompressedBitmap> join1 = new MutableObject<>(new EWAHCompressedBitmap());
        final List<EWAHCompressedBitmap> toBeORed = new ArrayList<>();
        stream(stream, answer, Optional.<EWAHCompressedBitmap>absent(), aggregateField1, query.retrieveFieldName1, new CallbackStream<TermCount>() {

            @Override
            public TermCount callback(TermCount v) throws Exception {
                if (v != null) {
                    Optional<MiruInvertedIndex> invertedIndex = lookupField1.getInvertedIndex(v.termId);
                    if (invertedIndex.isPresent()) {
                        //join1.setValue(join1.getValue().or(invertedIndex.get().getIndex()));
                        toBeORed.add(invertedIndex.get().getIndex());
                    }
                }
                return v;
            }
        });
        join1.setValue(FastAggregation.bufferedor(bitsetBufferSize, toBeORed.toArray(new EWAHCompressedBitmap[toBeORed.size()])));
        // at this point have all activity for all my documents in join1.

        // feeds us all users
        final MinMaxPriorityQueue<TermCount> userHeap = MinMaxPriorityQueue.orderedBy(new Comparator<TermCount>() {

            @Override
            public int compare(TermCount o1, TermCount o2) {
                return -Long.compare(o1.count, o2.count); // mimus to reverse :)
            }
        }).maximumSize(query.resultCount).create(); // overloaded :(
        stream(stream, join1.getValue(), Optional.<EWAHCompressedBitmap>absent(), aggregateField2, query.retrieveFieldName2, new CallbackStream<TermCount>() {

            @Override
            public TermCount callback(TermCount v) throws Exception {
                if (v != null) {
                    userHeap.add(v);
                }
                return v;
            }
        });
        final MutableObject<EWAHCompressedBitmap> join2 = new MutableObject<>(new EWAHCompressedBitmap());
        final BloomIndex bloomIndex = new BloomIndex(Hashing.murmur3_128(), 100000, 0.01f); // TODO fix so how
        toBeORed.clear();
        for (TermCount tc : userHeap) {
            Optional<MiruInvertedIndex> invertedIndex = lookupField2.getInvertedIndex(tc.termId);
            if (invertedIndex.isPresent()) {
                toBeORed.add(invertedIndex.get().getIndex());
            }
        }
        join2.setValue(FastAggregation.bufferedor(bitsetBufferSize, toBeORed.toArray(new EWAHCompressedBitmap[toBeORed.size()])));

        final List<TermCount> mostLike = new ArrayList<>(userHeap);
        final List<BloomIndex.Mights<TermCount>> wantBits = bloomIndex.wantBits(mostLike);
        // at this point have all activity for all users that have also touched my documents

        //join2.setValue(join2.getValue().and(authz.getValue())); // TODO
        join2.setValue(join2.getValue().andNot(join1.getValue())); // remove my activity from all activity around said documents

        final MinMaxPriorityQueue<TermCount> heap = MinMaxPriorityQueue.orderedBy(new Comparator<TermCount>() {

            @Override
            public int compare(TermCount o1, TermCount o2) {
                return -Long.compare(o1.count, o2.count); // mimus to reverse :)
            }
        }).maximumSize(query.resultCount).create();
        // feeds us all recommended documents
        final MutableLong tested = new MutableLong();
        stream(stream, join2.getValue(), Optional.<EWAHCompressedBitmap>absent(), aggregateField3, query.retrieveFieldName3, new CallbackStream<TermCount>() {

            @Override
            public TermCount callback(TermCount v) throws Exception {
                if (v != null) {

                    Optional<MiruInvertedIndex> invertedIndex = aggregateField3.getInvertedIndex(makeComposite(v.termId, "|", query.retrieveFieldName2));
                    if (invertedIndex.isPresent()) {
                        MiruInvertedIndex index = invertedIndex.get();
                        final MutableInt count = new MutableInt(0);
                        bloomIndex.mightContain(index, wantBits, new BloomIndex.MightContain<TermCount>() {

                            @Override
                            public void mightContain(TermCount value) {
                                count.add(value.count);
                            }
                        });
                        heap.add(new TermCount(v.termId, v.mostRecent, count.longValue()));

                        for(BloomIndex.Mights<TermCount> boo:wantBits) {
                            boo.reset();
                        }
                        tested.increment();
                    }

                }
                return v;
            }
        });

        System.out.println("Tested!="+tested.longValue());

        List<Recommendation> results = new ArrayList<>();
        for (TermCount result : heap) {
            results.add(new Recommendation(result.termId, result.count));
        }
        return new RecoResult(results);


    }

    RecoResult collaborativeFilteringMinusStreams(MiruQueryStream stream, final RecoQuery query, Optional<RecoReport> report, EWAHCompressedBitmap answer, int bitsetBufferSize) throws Exception {

        MiruField aggregateField1 = stream.fieldIndex.getField(stream.schema.getFieldId(query.aggregateFieldName1));
        final MiruField lookupField1 = stream.fieldIndex.getField(stream.schema.getFieldId(query.lookupFieldNamed1));

        MiruField aggregateField2 = stream.fieldIndex.getField(stream.schema.getFieldId(query.aggregateFieldName2));
        final MiruField lookupField2 = stream.fieldIndex.getField(stream.schema.getFieldId(query.lookupFieldNamed2));

        final MiruField aggregateField3 = stream.fieldIndex.getField(stream.schema.getFieldId(query.aggregateFieldName3));


        // feeds us our docIds
        final MutableObject<EWAHCompressedBitmap> join1 = new MutableObject<>(new EWAHCompressedBitmap());
        final List<EWAHCompressedBitmap> toBeORed = new ArrayList<>();
        IntIterator answerIterator = answer.intIterator();
        while (answerIterator.hasNext()) {
            int id = answerIterator.next();
            MiruActivity activity = stream.activityIndex.get(id);
            MiruTermId[] fieldValues = activity.fieldsValues.get(query.aggregateFieldName1);
            if (fieldValues != null && fieldValues.length > 0) {
                Optional<MiruInvertedIndex> invertedIndex = aggregateField1.getInvertedIndex(fieldValues[0]);
                if (invertedIndex.isPresent()) {
                    //join1.setValue(join1.getValue().or(invertedIndex.get().getIndex()));
                    toBeORed.add(invertedIndex.get().getIndex());
                }
            }
        }

        join1.setValue(FastAggregation.bufferedor(bitsetBufferSize, toBeORed.toArray(new EWAHCompressedBitmap[toBeORed.size()])));
        // at this point have all activity for all my documents in join1.

        // feeds us all users
        final MinMaxPriorityQueue<TermCount> userHeap = MinMaxPriorityQueue.orderedBy(new Comparator<TermCount>() {

            @Override
            public int compare(TermCount o1, TermCount o2) {
                return -Long.compare(o1.count, o2.count); // mimus to reverse :)
            }
        }).maximumSize(query.resultCount).create(); // overloaded :(

        stream(stream, join1.getValue(), Optional.<EWAHCompressedBitmap>absent(), aggregateField2, query.retrieveFieldName2, new CallbackStream<TermCount>() {

            @Override
            public TermCount callback(TermCount v) throws Exception {
                if (v != null) {
                    userHeap.add(v);
                }
                return v;
            }
        });
        final MutableObject<EWAHCompressedBitmap> join2 = new MutableObject<>(new EWAHCompressedBitmap());
        final BloomIndex bloomIndex = new BloomIndex(Hashing.murmur3_128(), 100000, 0.01f); // TODO fix so how
        toBeORed.clear();
        for (TermCount tc : userHeap) {
            Optional<MiruInvertedIndex> invertedIndex = lookupField2.getInvertedIndex(makeComposite(tc.termId, "^", "doc"));
            if (invertedIndex.isPresent()) {
                toBeORed.add(invertedIndex.get().getIndex());
            }
        }
        join2.setValue(FastAggregation.bufferedor(bitsetBufferSize, toBeORed.toArray(new EWAHCompressedBitmap[toBeORed.size()])));

        final List<TermCount> mostLike = new ArrayList<>(userHeap);
        final List<BloomIndex.Mights<TermCount>> wantBits = bloomIndex.wantBits(mostLike);
        // at this point have all activity for all users that have also touched my documents

        //join2.setValue(join2.getValue().and(authz.getValue())); // TODO
        join2.setValue(join2.getValue().andNot(join1.getValue())); // remove my activity from all activity around said documents

        final MinMaxPriorityQueue<TermCount> heap = MinMaxPriorityQueue.orderedBy(new Comparator<TermCount>() {

            @Override
            public int compare(TermCount o1, TermCount o2) {
                return -Long.compare(o1.count, o2.count); // mimus to reverse :)
            }
        }).maximumSize(query.resultCount).create();
        // feeds us all recommended documents
        final MutableLong tested = new MutableLong();

        IntIterator join2iterator = join2.getValue().intIterator();
        while (join2iterator.hasNext()) {
            int id = join2iterator.next();
            MiruActivity activity = stream.activityIndex.get(id);
            MiruTermId[] fieldValues = activity.fieldsValues.get(query.aggregateFieldName3);
            if (fieldValues != null && fieldValues.length > 0) {
                Optional<MiruInvertedIndex> invertedIndex = aggregateField3.getInvertedIndex(makeComposite(fieldValues[0], "|", query.retrieveFieldName2));
                if (invertedIndex.isPresent()) {
                    MiruInvertedIndex index = invertedIndex.get();
                    final MutableInt count = new MutableInt(0);
                    bloomIndex.mightContain(index, wantBits, new BloomIndex.MightContain<TermCount>() {

                        @Override
                        public void mightContain(TermCount value) {
                            count.add(value.count);
                        }
                    });
                    heap.add(new TermCount(fieldValues[0], null, count.longValue()));

                    for(BloomIndex.Mights<TermCount> boo:wantBits) {
                        boo.reset();
                    }
                    tested.increment();
                }
            }
        }

        System.out.println("Tested!="+tested.longValue());

        List<Recommendation> results = new ArrayList<>();
        for (TermCount result : heap) {
            results.add(new Recommendation(result.termId, result.count));
        }
        return new RecoResult(results);


    }

    private MiruTermId makeComposite(MiruTermId fieldValue, String separator, String fieldName) {
        return new MiruTermId(Bytes.concat(fieldValue.getBytes(), separator.getBytes(), fieldName.getBytes()));
    }

    private void stream(MiruQueryStream stream, EWAHCompressedBitmap answer,
            Optional<EWAHCompressedBitmap> counter,
            MiruField pivotField, String streamField, CallbackStream<TermCount> terms) throws Exception {

        AnswerCardinalityLastSetBitmapStorage answerCollector = null;
        ReusableBuffers reusable = new ReusableBuffers(2);
        int beforeCount = counter.isPresent() ? counter.get().cardinality() : answer.cardinality();
        while (true) {
            int lastSetBit = answerCollector == null ? lastSetBit(answer) : answerCollector.getLastSetBit();
            if (lastSetBit < 0) {
                break;
            }

            MiruActivity activity = stream.activityIndex.get(lastSetBit);
            MiruTermId[] fieldValues = activity.fieldsValues.get(streamField);
            if (fieldValues == null || fieldValues.length == 0) {
                // could make this a reusable buffer, but this is effectively an error case and would require 3 buffers
                EWAHCompressedBitmap removeUnknownField = new EWAHCompressedBitmap();
                removeUnknownField.set(lastSetBit);
                EWAHCompressedBitmap revisedAnswer = reusable.next();
                answerCollector = new AnswerCardinalityLastSetBitmapStorage(revisedAnswer);
                answer.andNotToContainer(removeUnknownField, answerCollector);
                answer = revisedAnswer;

            } else {
                MiruTermId pivotTerm = fieldValues[0]; // Kinda lame but for now we don't see a need for multi field aggregation.

                Optional<MiruInvertedIndex> invertedIndex = pivotField.getInvertedIndex(pivotTerm);
                checkState(invertedIndex.isPresent(), "Unable to load inverted index for aggregateTermId: " + pivotTerm);

                EWAHCompressedBitmap termIndex = invertedIndex.get().getIndex();

                EWAHCompressedBitmap revisedAnswer = reusable.next();
                answerCollector = new AnswerCardinalityLastSetBitmapStorage(revisedAnswer);
                answer.andNotToContainer(termIndex, answerCollector);
                answer = revisedAnswer;

                int afterCount;
                if (counter.isPresent()) {
                    EWAHCompressedBitmap revisedCounter = reusable.next();
                    AnswerCardinalityLastSetBitmapStorage counterCollector = new AnswerCardinalityLastSetBitmapStorage(revisedCounter);
                    counter.get().andNotToContainer(termIndex, counterCollector);
                    counter = Optional.of(revisedCounter);
                    afterCount = counterCollector.getCount();
                } else {
                    afterCount = answerCollector.getCount();
                }

                TermCount termCount = new TermCount(pivotTerm, activity, beforeCount - afterCount);
                if (termCount != terms.callback(termCount)) { // Stop stream
                    return;
                }
                beforeCount = afterCount;
            }

        }
        terms.callback(null); // EOS

    }

    public int lastSetBit(EWAHCompressedBitmap bitmap) {
        IntIterator iterator = bitmap.intIterator();
        int last = -1;
        for (; iterator.hasNext();) {
            last = iterator.next();
        }
        return last;
    }

    static class TermCount implements BloomIndex.HasValue {

        public final MiruTermId termId;
        public MiruActivity mostRecent;
        public final long count;

        public TermCount(MiruTermId termId, MiruActivity mostRecent, long count) {
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
