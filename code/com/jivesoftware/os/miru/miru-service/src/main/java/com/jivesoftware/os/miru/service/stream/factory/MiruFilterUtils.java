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

        int initialZeroWords = smallestId / EWAHCompressedBitmap.WORD_IN_BITS;
        if (initialZeroWords > 0) {
            mask.addStreamOfEmptyWords(false, initialZeroWords);
        }

        //TODO[LP] see if there's a way to simplify this logic
        if (largestId == smallestId) {
            // one bit to set
            mask.set(smallestId);
        } else if (largestId < (smallestId - smallestId % EWAHCompressedBitmap.WORD_IN_BITS + EWAHCompressedBitmap.WORD_IN_BITS)) {
            // start and stop in same word
            int firstOne = smallestId - initialZeroWords * EWAHCompressedBitmap.WORD_IN_BITS;
            int numberOfOnes = largestId - smallestId + 1;
            long word = 0;
            for (int i = 0; i < numberOfOnes; i++) {
                word |= (1l << (firstOne + i));
            }
            mask.addWord(word);
        } else if (largestId > smallestId) {
            // start word, run of ones, stop word
            int onesInStartWord = EWAHCompressedBitmap.WORD_IN_BITS - smallestId % EWAHCompressedBitmap.WORD_IN_BITS;
            if (onesInStartWord == EWAHCompressedBitmap.WORD_IN_BITS) {
                onesInStartWord = 0;
            }
            if (onesInStartWord > 0) {
                long startWord = 0;
                for (int i = 0; i < onesInStartWord; i++) {
                    startWord |= (1l << (63 - i));
                }
                mask.addWord(startWord);
            }

            int middleOneWords = (largestId - smallestId - onesInStartWord) / EWAHCompressedBitmap.WORD_IN_BITS;
            if (middleOneWords > 0) {
                mask.addStreamOfEmptyWords(true, middleOneWords);
            }

            int bitsInStopWord = largestId - smallestId + 1 - onesInStartWord - middleOneWords * EWAHCompressedBitmap.WORD_IN_BITS;
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

        int words = largestIndex / EWAHCompressedBitmap.WORD_IN_BITS;
        if (words > 0) {
            mask.addStreamOfEmptyWords(true, words);
        }

        int remainingBits = largestIndex % EWAHCompressedBitmap.WORD_IN_BITS + 1;
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

    /*  I have viewd these thing who has also view these things and what other thing have the viewed that I have not.*/
    RecoResult collaborativeFiltering(MiruQueryStream stream, final RecoQuery query, Optional<RecoReport> report, EWAHCompressedBitmap answer, int bitsetBufferSize) throws Exception {
        //return collaborativeFilteringWithStreams(stream, query, report, answer, bitsetBufferSize);
        return collaborativeFilteringMinusStreams(stream, query, report, answer, bitsetBufferSize);
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
        long startTime = System.currentTimeMillis();
        final MutableLong countDocToUser = new MutableLong();
        final MutableObject<EWAHCompressedBitmap> join1 = new MutableObject<>(new EWAHCompressedBitmap());
        final List<EWAHCompressedBitmap> toBeORed = new ArrayList<>();
        IntIterator answerIterator = answer.intIterator();
        while (answerIterator.hasNext()) {
            int id = answerIterator.next();
            MiruActivity activity = stream.activityIndex.get(id);
            MiruTermId[] fieldValues = activity.fieldsValues.get(query.aggregateFieldName1);
            if (fieldValues != null && fieldValues.length > 0) {
                Optional<MiruInvertedIndex> invertedIndex = aggregateField1.getInvertedIndex(makeComposite(fieldValues[0], "^", query.aggregateFieldName2));
                if (invertedIndex.isPresent()) {
                    toBeORed.add(invertedIndex.get().getIndex());
                    countDocToUser.increment();
                }
            }
        }
        long timeDocToUser = System.currentTimeMillis() - startTime;

        startTime = System.currentTimeMillis();
        join1.setValue(FastAggregation.bufferedor(bitsetBufferSize, toBeORed.toArray(new EWAHCompressedBitmap[toBeORed.size()])));
        long timeJoin1 = System.currentTimeMillis() - startTime;
        // at this point have all activity for all my documents in join1.

        // feeds us all users
        final MinMaxPriorityQueue<TermCount> userHeap = MinMaxPriorityQueue.orderedBy(new Comparator<TermCount>() {

            @Override
            public int compare(TermCount o1, TermCount o2) {
                return -Long.compare(o1.count, o2.count); // mimus to reverse :)
            }
        }).maximumSize(query.resultCount).create(); // overloaded :(

        final MutableLong countUserToHeap = new MutableLong();
        final MutableLong sizeUserToHeap = new MutableLong();
        startTime = System.currentTimeMillis();
        stream(stream, join1.getValue(), Optional.<EWAHCompressedBitmap>absent(), aggregateField2, query.retrieveFieldName2, new CallbackStream<TermCount>() {

            @Override
            public TermCount callback(TermCount v) throws Exception {
                if (v != null) {
                    userHeap.add(v);
                    countUserToHeap.increment();
                    sizeUserToHeap.add(v.count);
                }
                return v;
            }
        });
        long timeUserToHeap = System.currentTimeMillis() - startTime;

        final MutableObject<EWAHCompressedBitmap> join2 = new MutableObject<>(new EWAHCompressedBitmap());
        final BloomIndex bloomIndex = new BloomIndex(Hashing.murmur3_128(), 100000, 0.01f); // TODO fix so how
        toBeORed.clear();
        for (TermCount tc : userHeap) {
            Optional<MiruInvertedIndex> invertedIndex = lookupField2.getInvertedIndex(makeComposite(tc.termId, "^", query.aggregateFieldName3));
            if (invertedIndex.isPresent()) {
                toBeORed.add(invertedIndex.get().getIndex());
            }
        }
        startTime = System.currentTimeMillis();
        join2.setValue(FastAggregation.bufferedor(bitsetBufferSize, toBeORed.toArray(new EWAHCompressedBitmap[toBeORed.size()])));
        long timeJoin2 = System.currentTimeMillis() - startTime;

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
        final MutableLong countDocToBloom = new MutableLong();
        startTime = System.currentTimeMillis();

        IntIterator join2iterator = join2.getValue().intIterator();
        while (join2iterator.hasNext()) {
            int id = join2iterator.next();
            MiruActivity activity = stream.activityIndex.get(id);
            MiruTermId[] fieldValues = activity.fieldsValues.get(query.retrieveFieldName3);
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
                    countDocToBloom.increment();
                }
            }
        }

        long timeDocToBloom = System.currentTimeMillis() - startTime;

        /*
        int numberOfUsers = 10_000;
        int numberOfDocument = 1_000;
        int numberOfViewsPerUser = 100;
        CountDocToUser=97 SizeJoin1=92316 CountUserToHeap=10001 SizeUserToHeap=92316 SizeJoin2=680 CountDocToBloom=680
        TimeDocToUser=1 TimeJoin1=1 TimeUserToHeap=119 TimeJoin2=1 TimeDocToBloom=70
        recoResult:TrendingResult{results=[Recommendation{, distinctValue=46, rank=81.0}, Recommendation{, distinctValue=46, rank=81.0}, Recommendation{, distinctValue=157, rank=81.0}, Recommendation{, distinctValue=157, rank=81.0}, Recommendation{, distinctValue=46, rank=81.0}, Recommendation{, distinctValue=46, rank=81.0}, Recommendation{, distinctValue=157, rank=81.0}, Recommendation{, distinctValue=157, rank=81.0}, Recommendation{, distinctValue=20, rank=80.0}, Recommendation{, distinctValue=20, rank=80.0}]}
        Took:192
        CountDocToUser=96 SizeJoin1=91435 CountUserToHeap=10002 SizeUserToHeap=91435 SizeJoin2=691 CountDocToBloom=691
        TimeDocToUser=0 TimeJoin1=1 TimeUserToHeap=129 TimeJoin2=0 TimeDocToBloom=72
        recoResult:TrendingResult{results=[Recommendation{, distinctValue=976, rank=79.0}, Recommendation{, distinctValue=976, rank=79.0}, Recommendation{, distinctValue=976, rank=79.0}, Recommendation{, distinctValue=976, rank=79.0}, Recommendation{, distinctValue=472, rank=78.0}, Recommendation{, distinctValue=472, rank=78.0}, Recommendation{, distinctValue=472, rank=78.0}, Recommendation{, distinctValue=472, rank=78.0}, Recommendation{, distinctValue=807, rank=77.0}, Recommendation{, distinctValue=807, rank=77.0}]}
        Took:203
        CountDocToUser=95 SizeJoin1=90765 CountUserToHeap=10001 SizeUserToHeap=90765 SizeJoin2=704 CountDocToBloom=704
        TimeDocToUser=0 TimeJoin1=2 TimeUserToHeap=126 TimeJoin2=1 TimeDocToBloom=69
        recoResult:TrendingResult{results=[Recommendation{, distinctValue=532, rank=76.0}, Recommendation{, distinctValue=532, rank=76.0}, Recommendation{, distinctValue=773, rank=76.0}, Recommendation{, distinctValue=73, rank=76.0}, Recommendation{, distinctValue=773, rank=76.0}, Recommendation{, distinctValue=73, rank=76.0}, Recommendation{, distinctValue=532, rank=76.0}, Recommendation{, distinctValue=773, rank=76.0}, Recommendation{, distinctValue=73, rank=76.0}, Recommendation{, distinctValue=532, rank=76.0}]}
        Took:198
         */
        /*
        int numberOfUsers = 10_000;
        int numberOfDocument = 10_000;
        int numberOfViewsPerUser = 100;
        CountDocToUser=98 SizeJoin1=9809 CountUserToHeap=6285 SizeUserToHeap=9809 SizeJoin2=846 CountDocToBloom=846
        TimeDocToUser=0 TimeJoin1=0 TimeUserToHeap=708 TimeJoin2=0 TimeDocToBloom=11
        recoResult:TrendingResult{results=[Recommendation{, distinctValue=9004, rank=11.0}, Recommendation{, distinctValue=1782, rank=11.0}, Recommendation{, distinctValue=9181, rank=11.0}, Recommendation{, distinctValue=9181, rank=11.0}, Recommendation{, distinctValue=1782, rank=11.0}, Recommendation{, distinctValue=7494, rank=11.0}, Recommendation{, distinctValue=4514, rank=11.0}, Recommendation{, distinctValue=9004, rank=11.0}, Recommendation{, distinctValue=4514, rank=11.0}, Recommendation{, distinctValue=5246, rank=11.0}]}
        Took:721
        CountDocToUser=97 SizeJoin1=9765 CountUserToHeap=6263 SizeUserToHeap=9765 SizeJoin2=846 CountDocToBloom=846
        TimeDocToUser=0 TimeJoin1=1 TimeUserToHeap=681 TimeJoin2=0 TimeDocToBloom=11
        recoResult:TrendingResult{results=[Recommendation{, distinctValue=9795, rank=12.0}, Recommendation{, distinctValue=9795, rank=12.0}, Recommendation{, distinctValue=5473, rank=12.0}, Recommendation{, distinctValue=4813, rank=12.0}, Recommendation{, distinctValue=3923, rank=12.0}, Recommendation{, distinctValue=1418, rank=12.0}, Recommendation{, distinctValue=6381, rank=12.0}, Recommendation{, distinctValue=1418, rank=12.0}, Recommendation{, distinctValue=6381, rank=12.0}, Recommendation{, distinctValue=3923, rank=12.0}]}
        Took:694
        CountDocToUser=100 SizeJoin1=9941 CountUserToHeap=6272 SizeUserToHeap=9941 SizeJoin2=847 CountDocToBloom=847
        TimeDocToUser=0 TimeJoin1=1 TimeUserToHeap=686 TimeJoin2=0 TimeDocToBloom=11
        recoResult:TrendingResult{results=[Recommendation{, distinctValue=140, rank=13.0}, Recommendation{, distinctValue=140, rank=13.0}, Recommendation{, distinctValue=9723, rank=13.0}, Recommendation{, distinctValue=9723, rank=13.0}, Recommendation{, distinctValue=8578, rank=12.0}, Recommendation{, distinctValue=8578, rank=12.0}, Recommendation{, distinctValue=9535, rank=12.0}, Recommendation{, distinctValue=4960, rank=12.0}, Recommendation{, distinctValue=2243, rank=12.0}, Recommendation{, distinctValue=6065, rank=12.0}]}
        Took:698
         */

        System.out.println("CountDocToUser=" + countDocToUser.longValue() +
                " SizeJoin1=" + join1.getValue().cardinality() +
                " CountUserToHeap=" + countUserToHeap.longValue() +
                " SizeUserToHeap=" + sizeUserToHeap.longValue() +
                " SizeJoin2=" + join2.getValue().cardinality() +
                " CountDocToBloom=" + countDocToBloom.longValue());

        System.out.println("TimeDocToUser=" + timeDocToUser +
                " TimeJoin1=" + timeJoin1 +
                " TimeUserToHeap=" + timeUserToHeap +
                " TimeJoin2=" + timeJoin2 +
                " TimeDocToBloom=" + timeDocToBloom);

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
