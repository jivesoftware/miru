package com.jivesoftware.os.miru.stream.plugins.filter;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.plugin.MiruProvider;
import com.jivesoftware.os.miru.plugin.bitmap.CardinalityAndLastSetBit;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmapsDebug;
import com.jivesoftware.os.miru.plugin.bitmap.ReusableBuffers;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInternalActivity;
import com.jivesoftware.os.miru.plugin.index.MiruTermComposer;
import com.jivesoftware.os.miru.plugin.solution.MiruAggregateUtil;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;

/**
 *
 */
public class AggregateCounts {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final MiruAggregateUtil aggregateUtil = new MiruAggregateUtil();
    private final MiruBitmapsDebug bitmapsDebug = new MiruBitmapsDebug();
    private final MiruProvider miruProvider;

    public AggregateCounts(MiruProvider miruProvider) {
        this.miruProvider = miruProvider;
    }

    public <BM extends IBM, IBM> AggregateCountsAnswer getAggregateCounts(MiruSolutionLog solutionLog,
        MiruBitmaps<BM, IBM> bitmaps,
        MiruRequestContext<IBM, ?> requestContext,
        MiruRequest<AggregateCountsQuery> request,
        Optional<AggregateCountsReport> lastReport,
        BM answer,
        Optional<BM> counter)
        throws Exception {

        log.debug("Get aggregate counts for answer={} request={}", answer, request);

        Map<String, AggregateCountsAnswerConstraint> results = Maps.newHashMapWithExpectedSize(request.query.constraints.size());
        for (Map.Entry<String, AggregateCountsQueryConstraint> entry : request.query.constraints.entrySet()) {

            Optional<AggregateCountsReportConstraint> lastReportConstraint = Optional.absent();
            if (lastReport.isPresent()) {
                lastReportConstraint = Optional.of(lastReport.get().constraints.get(entry.getKey()));
            }

            results.put(entry.getKey(),
                answerConstraint(solutionLog,
                    bitmaps,
                    requestContext,
                    request.tenantId,
                    request.query.streamId,
                    entry.getValue(),
                    lastReportConstraint,
                    answer,
                    counter));
        }

        boolean resultsExhausted = request.query.answerTimeRange.smallestTimestamp > requestContext.getTimeIndex().getLargestTimestamp();
        AggregateCountsAnswer result = new AggregateCountsAnswer(results, resultsExhausted);
        log.debug("result={}", result);
        return result;
    }

    private final <BM extends IBM, IBM> AggregateCountsAnswerConstraint answerConstraint(MiruSolutionLog solutionLog,
        MiruBitmaps<BM, IBM> bitmaps,
        MiruRequestContext<IBM, ?> requestContext,
        MiruTenantId tenantId,
        MiruStreamId streamId,
        AggregateCountsQueryConstraint constraint,
        Optional<AggregateCountsReportConstraint> lastReport,
        BM answer,
        Optional<BM> counter) throws Exception {

        StackBuffer stackBuffer = new StackBuffer();

        int collectedDistincts = 0;
        int skippedDistincts = 0;
        Set<String> aggregateTerms;
        if (lastReport.isPresent()) {
            collectedDistincts = lastReport.get().collectedDistincts;
            skippedDistincts = lastReport.get().skippedDistincts;
            aggregateTerms = Sets.newHashSet(lastReport.get().aggregateTerms);
        } else {
            aggregateTerms = Sets.newHashSet();
        }

        if (!MiruFilter.NO_FILTER.equals(constraint.constraintsFilter)) {
            BM filtered = aggregateUtil.filter(bitmaps, requestContext.getSchema(), requestContext.getTermComposer(), requestContext.getFieldIndexProvider(),
                constraint.constraintsFilter, solutionLog, null, requestContext.getActivityIndex().lastId(stackBuffer), -1, stackBuffer);

            if (bitmaps.supportsInPlace()) {
                bitmaps.inPlaceAnd(answer, filtered);

                if (counter.isPresent()) {
                    bitmaps.inPlaceAnd(counter.get(), filtered);
                }
            } else {
                BM constrained = bitmaps.create();
                List<IBM> ands = Arrays.asList(answer, filtered);
                bitmapsDebug.debug(solutionLog, bitmaps, "ands", ands);
                bitmaps.and(constrained, ands);
                answer = constrained;

                if (counter.isPresent()) {
                    constrained = bitmaps.create();
                    ands = Arrays.asList(counter.get(), filtered);
                    bitmapsDebug.debug(solutionLog, bitmaps, "ands", ands);
                    bitmaps.and(constrained, ands);
                    counter = Optional.of(constrained);
                }
            }
        }

        List<AggregateCount> aggregateCounts = new ArrayList<>();
        MiruTermComposer termComposer = requestContext.getTermComposer();
        MiruFieldIndex<IBM> fieldIndex = requestContext.getFieldIndexProvider().getFieldIndex(MiruFieldType.primary);
        int fieldId = requestContext.getSchema().getFieldId(constraint.aggregateCountAroundField);
        MiruFieldDefinition fieldDefinition = requestContext.getSchema().getFieldDefinition(fieldId);
        log.debug("fieldId={}", fieldId);
        if (fieldId >= 0) {
            IBM unreadIndex = null;
            if (!MiruStreamId.NULL.equals(streamId)) {
                Optional<IBM> unread = requestContext.getUnreadTrackingIndex().getUnread(streamId).getIndex(stackBuffer);
                if (unread.isPresent()) {
                    unreadIndex = unread.get();
                }
            }

            // 2 to swap answers, 2 to swap counters, 1 to check unread
            final int numBuffers = 2 + (counter.isPresent() ? 2 : 0) + (unreadIndex != null ? 1 : 0);
            ReusableBuffers<BM, IBM> reusable = new ReusableBuffers<>(bitmaps, numBuffers);

            long beforeCount = counter.isPresent() ? bitmaps.cardinality(counter.get()) : bitmaps.cardinality(answer);
            CardinalityAndLastSetBit answerCollector = null;
            for (String aggregateTerm : aggregateTerms) { // Consider
                MiruTermId aggregateTermId = termComposer.compose(fieldDefinition, aggregateTerm);
                Optional<IBM> optionalTermIndex = fieldIndex.get(fieldId, aggregateTermId).getIndex(stackBuffer);
                if (!optionalTermIndex.isPresent()) {
                    continue;
                }

                IBM termIndex = optionalTermIndex.get();

                if (bitmaps.supportsInPlace()) {
                    answerCollector = bitmaps.inPlaceAndNotWithCardinalityAndLastSetBit(answer, termIndex);
                } else {
                    BM revisedAnswer = reusable.next();
                    answerCollector = bitmaps.andNotWithCardinalityAndLastSetBit(revisedAnswer, answer, termIndex);
                    answer = revisedAnswer;
                }

                long afterCount;
                if (counter.isPresent()) {
                    if (bitmaps.supportsInPlace()) {
                        CardinalityAndLastSetBit counterCollector = bitmaps.inPlaceAndNotWithCardinalityAndLastSetBit(counter.get(), termIndex);
                        afterCount = counterCollector.cardinality;
                    } else {
                        BM revisedCounter = reusable.next();
                        CardinalityAndLastSetBit counterCollector = bitmaps.andNotWithCardinalityAndLastSetBit(revisedCounter, counter.get(), termIndex);
                        counter = Optional.of(revisedCounter);
                        afterCount = counterCollector.cardinality;
                    }
                } else {
                    afterCount = answerCollector.cardinality;
                }

                boolean unread = false;
                if (unreadIndex != null) {
                    //TODO much more efficient to add a bitmaps.intersects() to test for first bit in common
                    BM unreadAnswer = reusable.next();
                    CardinalityAndLastSetBit storage = bitmaps.andWithCardinalityAndLastSetBit(unreadAnswer, Arrays.asList(unreadIndex, termIndex));
                    if (storage.cardinality > 0) {
                        unread = true;
                    }
                }

                aggregateCounts.add(new AggregateCount(null, aggregateTerm, beforeCount - afterCount, unread));
                beforeCount = afterCount;
            }

            while (true) {
                int lastSetBit = answerCollector == null ? bitmaps.lastSetBit(answer) : answerCollector.lastSetBit;
                log.trace("lastSetBit={}", lastSetBit);
                if (lastSetBit < 0) {
                    break;
                }

                MiruInternalActivity activity = requestContext.getActivityIndex().get(tenantId, lastSetBit, stackBuffer);
                MiruTermId[] fieldValues = activity.fieldsValues[fieldId];
                log.trace("fieldValues={}", (Object) fieldValues);
                if (fieldValues == null || fieldValues.length == 0) {
                    // could make this a reusable buffer, but this is effectively an error case and would require 3 buffers
                    if (bitmaps.supportsInPlace()) {
                        BM removeUnknownField = bitmaps.createWithBits(lastSetBit);
                        answerCollector = bitmaps.inPlaceAndNotWithCardinalityAndLastSetBit(answer, removeUnknownField);
                    } else {
                        BM removeUnknownField = bitmaps.createWithBits(lastSetBit);
                        BM revisedAnswer = reusable.next();
                        answerCollector = bitmaps.andNotWithCardinalityAndLastSetBit(revisedAnswer, answer, removeUnknownField);
                        answer = revisedAnswer;
                    }
                    beforeCount--;

                } else {
                    MiruTermId aggregateTermId = fieldValues[0]; // Kinda lame but for now we don't see a need for multi field aggregation.
                    String aggregateTerm = termComposer.decompose(fieldDefinition, aggregateTermId);
                    aggregateTerms.add(aggregateTerm);

                    Optional<IBM> optionalTermIndex = fieldIndex.get(fieldId, aggregateTermId).getIndex(stackBuffer);
                    checkState(optionalTermIndex.isPresent(), "Unable to load inverted index for aggregateTermId: " + aggregateTermId);

                    IBM termIndex = optionalTermIndex.get();

                    if (bitmaps.supportsInPlace()) {
                        answerCollector = bitmaps.inPlaceAndNotWithCardinalityAndLastSetBit(answer, termIndex);
                    } else {
                        BM revisedAnswer = reusable.next();
                        answerCollector = bitmaps.andNotWithCardinalityAndLastSetBit(revisedAnswer, answer, termIndex);
                        answer = revisedAnswer;
                    }

                    long afterCount;
                    if (counter.isPresent()) {
                        if (bitmaps.supportsInPlace()) {
                            CardinalityAndLastSetBit counterCollector = bitmaps.inPlaceAndNotWithCardinalityAndLastSetBit(counter.get(), termIndex);
                            afterCount = counterCollector.cardinality;
                        } else {
                            BM revisedCounter = reusable.next();
                            CardinalityAndLastSetBit counterCollector = bitmaps.andNotWithCardinalityAndLastSetBit(revisedCounter, counter.get(), termIndex);
                            counter = Optional.of(revisedCounter);
                            afterCount = counterCollector.cardinality;
                        }
                    } else {
                        afterCount = answerCollector.cardinality;
                    }

                    collectedDistincts++;
                    if (collectedDistincts > constraint.startFromDistinctN) {
                        boolean unread = false;
                        if (unreadIndex != null) {
                            //TODO much more efficient to add a bitmaps.intersects() to test for first bit in common
                            BM unreadAnswer = reusable.next();
                            CardinalityAndLastSetBit storage = bitmaps.andWithCardinalityAndLastSetBit(unreadAnswer, Arrays.asList(unreadIndex, termIndex));
                            if (storage.cardinality > 0) {
                                unread = true;
                            }
                        }

                        AggregateCount aggregateCount = new AggregateCount(
                            miruProvider.getActivityInternExtern(tenantId).extern(activity, requestContext.getSchema()),
                            aggregateTerm,
                            beforeCount - afterCount,
                            unread);
                        aggregateCounts.add(aggregateCount);

                        if (aggregateCounts.size() >= constraint.desiredNumberOfDistincts) {
                            break;
                        }
                    } else {
                        skippedDistincts++;
                    }
                    beforeCount = afterCount;
                }
            }
        }
        return new AggregateCountsAnswerConstraint(aggregateCounts, aggregateTerms, skippedDistincts, collectedDistincts);
    }

}
