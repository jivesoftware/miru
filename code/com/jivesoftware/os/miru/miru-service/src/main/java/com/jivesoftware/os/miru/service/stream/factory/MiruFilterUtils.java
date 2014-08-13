package com.jivesoftware.os.miru.service.stream.factory;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Sets;
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
import com.jivesoftware.os.miru.service.index.MiruField;
import com.jivesoftware.os.miru.service.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.service.index.MiruTimeIndex;
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
                int lastSetBit = answerCollector == null ? stream.ewahUtils.lastSetBit(answer) : answerCollector.getLastSetBit();
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
                int lastSetBit = answerCollector == null ? stream.ewahUtils.lastSetBit(answer) : answerCollector.getLastSetBit();
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
                int lastSetBit = answerCollector == null ? stream.ewahUtils.lastSetBit(answer) : answerCollector.getLastSetBit();
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

    /*  I have viewd these thing who has also view these things and what other thing have the viewed that I have not.*/
    RecoResult collaborativeFiltering(MiruQueryStream stream, RecoQuery query, Optional<RecoReport> report, EWAHCompressedBitmap answer) throws Exception {

        MiruField aggregateField1 = stream.fieldIndex.getField(stream.schema.getFieldId(query.aggregateFieldName1));
        final MiruField lookupField1 = stream.fieldIndex.getField(stream.schema.getFieldId(query.lookupFieldNamed1));

        MiruField aggregateField2 = stream.fieldIndex.getField(stream.schema.getFieldId(query.aggregateFieldName2));
        final MiruField lookupField2 = stream.fieldIndex.getField(stream.schema.getFieldId(query.lookupFieldNamed2));

        MiruField aggregateField3 = stream.fieldIndex.getField(stream.schema.getFieldId(query.aggregateFieldName3));

        // feeds us our docIds
        final MutableObject<EWAHCompressedBitmap> join1 = new MutableObject<>(new EWAHCompressedBitmap());
        stream(stream, answer, Optional.<EWAHCompressedBitmap>absent(), aggregateField1, query.retrieveFieldName1, new CallbackStream<TermCount>() {

            @Override
            public TermCount callback(TermCount v) throws Exception {
                if (v != null) {
                    Optional<MiruInvertedIndex> invertedIndex = lookupField1.getInvertedIndex(v.termId);
                    if (invertedIndex.isPresent()) {
                        join1.setValue(join1.getValue().or(invertedIndex.get().getIndex()));
                    }
                }
                return v;
            }
        });
        // at this point have all activity for all my documents in join1.

        // feeds us all users
        final MutableObject<EWAHCompressedBitmap> join2 = new MutableObject<>(new EWAHCompressedBitmap());
        stream(stream, join1.getValue(), Optional.<EWAHCompressedBitmap>absent(), aggregateField2, query.retrieveFieldName2, new CallbackStream<TermCount>() {

            @Override
            public TermCount callback(TermCount v) throws Exception {
                if (v != null) {
                    Optional<MiruInvertedIndex> invertedIndex = lookupField2.getInvertedIndex(v.termId);
                    if (invertedIndex.isPresent()) {
                        join2.setValue(join2.getValue().or(invertedIndex.get().getIndex()));
                    }
                }
                return v;
            }
        });
        // at this point have all activity for all users that have also touched my documents

        //join2.setValue(join2.getValue().and(authz.getValue())); // TODO
        join2.setValue(join2.getValue().andNot(join1.getValue())); // remove my activity from all activity around said documents

        // feeds us all recommended documents
        final MinMaxPriorityQueue<TermCount> heap = MinMaxPriorityQueue.orderedBy(new Comparator<TermCount>() {

            @Override
            public int compare(TermCount o1, TermCount o2) {
                return -Long.compare(o1.count, o2.count); // mimus to reverse :)
            }
        }).maximumSize(query.resultCount).create();
        stream(stream, join2.getValue(), Optional.<EWAHCompressedBitmap>absent(), aggregateField3, query.retrieveFieldName3, new CallbackStream<TermCount>() {

            @Override
            public TermCount callback(TermCount v) throws Exception {
                if (v != null) {
                    heap.add(v);
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

    private void stream(MiruQueryStream stream, EWAHCompressedBitmap answer,
            Optional<EWAHCompressedBitmap> counter,
            MiruField pivotField, String streamField, CallbackStream<TermCount> terms) throws Exception {

        AnswerCardinalityLastSetBitmapStorage answerCollector = null;
        ReusableBuffers reusable = new ReusableBuffers(2);
        int beforeCount = counter.isPresent() ? counter.get().cardinality() : answer.cardinality();
        while (true) {
            int lastSetBit = answerCollector == null ? stream.ewahUtils.lastSetBit(answer) : answerCollector.getLastSetBit();
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

    static class TermCount {

        public final MiruTermId termId;
        public MiruActivity mostRecent;
        public final long count;

        public TermCount(MiruTermId termId, MiruActivity mostRecent, long count) {
            this.termId = termId;
            this.mostRecent = mostRecent;
            this.count = count;
        }

    }

    /**
     * Cycles between buffers with the expectation that each buffer is derived from no more than "size - 1" previous buffers. For example, to aggregate a
     * previous reusable answer plus an additional reusable bitmap into a new answer, "size" must be at least 3. However, if the previous answer is reusable but
     * the additional bitmap is non-reusable, then "size" need only be 2.
     */
    private static class ReusableBuffers {

        private int index = 0;
        private final EWAHCompressedBitmap[] bufs;

        private ReusableBuffers(int size) {
            this.bufs = new EWAHCompressedBitmap[size];
            for (int i = 0; i < size; i++) {
                bufs[i] = new EWAHCompressedBitmap();
            }
        }

        EWAHCompressedBitmap next() {
            EWAHCompressedBitmap buf = bufs[index++ % bufs.length];
            buf.clear();
            return buf;
        }
    }
}
