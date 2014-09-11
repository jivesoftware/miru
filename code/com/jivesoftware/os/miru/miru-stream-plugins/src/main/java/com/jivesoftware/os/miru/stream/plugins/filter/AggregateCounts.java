package com.jivesoftware.os.miru.stream.plugins.filter;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.query.MiruProvider;
import com.jivesoftware.os.miru.query.bitmap.CardinalityAndLastSetBit;
import com.jivesoftware.os.miru.query.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.query.bitmap.ReusableBuffers;
import com.jivesoftware.os.miru.query.context.MiruRequestContext;
import com.jivesoftware.os.miru.query.index.MiruField;
import com.jivesoftware.os.miru.query.index.MiruInternalActivity;
import com.jivesoftware.os.miru.query.index.MiruInvertedIndex;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static com.jivesoftware.os.miru.stream.plugins.filter.AggregateCountsAnswer.AggregateCount;

/**
 *
 */
public class AggregateCounts {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final MiruProvider miruProvider;

    public AggregateCounts(MiruProvider miruProvider) {
        this.miruProvider = miruProvider;
    }

    public <BM> AggregateCountsAnswer getAggregateCounts(MiruBitmaps<BM> bitmaps,
            MiruRequestContext<BM> requestContext,
            AggregateCountsQuery query,
            Optional<AggregateCountsReport> lastReport,
            BM answer,
            Optional<BM> counter)
            throws Exception {

        log.debug("Get aggregate counts for answer={} query={}", answer, query);

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
        int fieldId = requestContext.schema.getFieldId(query.aggregateCountAroundField);
        log.debug("fieldId={}", fieldId);
        if (fieldId >= 0) {
            MiruField<BM> aggregateField = requestContext.fieldIndex.getField(fieldId);

            BM unreadIndex = null;
            if (!MiruStreamId.NULL.equals(query.streamId)) {
                Optional<BM> unread = requestContext.unreadTrackingIndex.getUnread(query.streamId);
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
                int lastSetBit = answerCollector == null ? bitmaps.lastSetBit(answer) : answerCollector.lastSetBit;
                log.trace("lastSetBit={}", lastSetBit);
                if (lastSetBit < 0) {
                    break;
                }

                MiruInternalActivity activity = requestContext.activityIndex.get(query.tenantId, lastSetBit);
                MiruTermId[] fieldValues = activity.fieldsValues[fieldId];
                log.trace("fieldValues={}", (Object) fieldValues);
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

                        AggregateCount aggregateCount = new AggregateCount(
                                miruProvider.getActivityInternExtern(query.tenantId).extern(activity, requestContext.schema),
                                aggregateValue,
                                beforeCount - afterCount,
                                unread);
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

        AggregateCountsAnswer result = new AggregateCountsAnswer(ImmutableList.copyOf(aggregateCounts), ImmutableSet.copyOf(aggregateTerms),
                skippedDistincts, collectedDistincts);
        log.debug("result={}", result);
        return result;
    }

}
