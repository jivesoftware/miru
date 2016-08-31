package com.jivesoftware.os.miru.stream.plugins.filter;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition.Feature;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruValue;
import com.jivesoftware.os.miru.plugin.MiruProvider;
import com.jivesoftware.os.miru.plugin.bitmap.CardinalityAndLastSetBit;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmapsDebug;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruTermComposer;
import com.jivesoftware.os.miru.plugin.index.TimeVersionRealtime;
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

    public <BM extends IBM, IBM> AggregateCountsAnswer getAggregateCounts(String name,
        MiruSolutionLog solutionLog,
        MiruBitmaps<BM, IBM> bitmaps,
        MiruRequestContext<BM, IBM, ?> requestContext,
        MiruRequest<AggregateCountsQuery> request,
        MiruPartitionCoord coord,
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
                answerConstraint(name,
                    solutionLog,
                    bitmaps,
                    requestContext,
                    coord,
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

    private <BM extends IBM, IBM> AggregateCountsAnswerConstraint answerConstraint(String name,
        MiruSolutionLog solutionLog,
        MiruBitmaps<BM, IBM> bitmaps,
        MiruRequestContext<BM, IBM, ?> requestContext,
        MiruPartitionCoord coord,
        MiruStreamId streamId,
        AggregateCountsQueryConstraint constraint,
        Optional<AggregateCountsReportConstraint> lastReport,
        BM answer,
        Optional<BM> counter) throws Exception {

        StackBuffer stackBuffer = new StackBuffer();

        MiruSchema schema = requestContext.getSchema();
        int fieldId = schema.getFieldId(constraint.aggregateCountAroundField);
        MiruFieldDefinition fieldDefinition = schema.getFieldDefinition(fieldId);
        Preconditions.checkArgument(fieldDefinition.type.hasFeature(Feature.stored), "You can only aggregate stored fields");

        int[] gatherFieldIds;
        if (constraint.gatherTermsForFields != null && constraint.gatherTermsForFields.length > 0) {
            gatherFieldIds = new int[constraint.gatherTermsForFields.length];
            for (int i = 0; i < gatherFieldIds.length; i++) {
                gatherFieldIds[i] = schema.getFieldId(constraint.gatherTermsForFields[i]);
                Preconditions.checkArgument(schema.getFieldDefinition(gatherFieldIds[i]).type.hasFeature(Feature.stored),
                    "You can only gather stored fields");
            }
        } else {
            gatherFieldIds = new int[0];
        }

        if (bitmaps.supportsInPlace()) {
            // don't mutate the original
            answer = bitmaps.copy(answer);
        }

        int collectedDistincts = 0;
        int skippedDistincts = 0;
        Set<MiruValue> aggregated;
        if (lastReport.isPresent()) {
            collectedDistincts = lastReport.get().collectedDistincts;
            skippedDistincts = lastReport.get().skippedDistincts;
            aggregated = Sets.newHashSet(lastReport.get().aggregateTerms);
        } else {
            aggregated = Sets.newHashSet();
        }

        if (!MiruFilter.NO_FILTER.equals(constraint.constraintsFilter)) {
            BM filtered = aggregateUtil.filter(name, bitmaps, schema, requestContext.getTermComposer(), requestContext.getFieldIndexProvider(),
                constraint.constraintsFilter, solutionLog, null, requestContext.getActivityIndex().lastId(stackBuffer), -1, stackBuffer);

            if (bitmaps.supportsInPlace()) {
                bitmaps.inPlaceAnd(answer, filtered);

                if (counter.isPresent()) {
                    bitmaps.inPlaceAnd(counter.get(), filtered);
                }
            } else {
                List<IBM> ands = Arrays.asList(answer, filtered);
                bitmapsDebug.debug(solutionLog, bitmaps, "ands", ands);
                answer = bitmaps.and(ands);

                if (counter.isPresent()) {
                    ands = Arrays.asList(counter.get(), filtered);
                    bitmapsDebug.debug(solutionLog, bitmaps, "ands", ands);
                    counter = Optional.of(bitmaps.and(ands));
                }
            }
        }

        MiruTermComposer termComposer = requestContext.getTermComposer();
        //MiruActivityInternExtern activityInternExtern = miruProvider.getActivityInternExtern(coord.tenantId);

        MiruFieldIndex<BM, IBM> fieldIndex = requestContext.getFieldIndexProvider().getFieldIndex(MiruFieldType.primary);
        log.debug("fieldId={}", fieldId);

        List<AggregateCount> aggregateCounts = new ArrayList<>();
        if (fieldId >= 0) {
            IBM unreadIndex = null;
            if (!MiruStreamId.NULL.equals(streamId)) {
                Optional<BM> unread = requestContext.getUnreadTrackingIndex().getUnread(streamId).getIndex(stackBuffer);
                if (unread.isPresent()) {
                    unreadIndex = unread.get();
                }
            }

            long beforeCount = counter.isPresent() ? bitmaps.cardinality(counter.get()) : bitmaps.cardinality(answer);
            CardinalityAndLastSetBit<BM> answerCollector = null;
            for (MiruValue aggregateTerm : aggregated) {
                MiruTermId aggregateTermId = termComposer.compose(schema, fieldDefinition, stackBuffer, aggregateTerm.parts);
                Optional<BM> optionalTermIndex = fieldIndex.get(name, fieldId, aggregateTermId).getIndex(stackBuffer);
                if (!optionalTermIndex.isPresent()) {
                    continue;
                }

                IBM termIndex = optionalTermIndex.get();

                if (bitmaps.supportsInPlace()) {
                    answerCollector = bitmaps.inPlaceAndNotWithCardinalityAndLastSetBit(answer, termIndex);
                } else {
                    answerCollector = bitmaps.andNotWithCardinalityAndLastSetBit(answer, termIndex);
                    answer = answerCollector.bitmap;
                }

                long afterCount;
                if (counter.isPresent()) {
                    if (bitmaps.supportsInPlace()) {
                        CardinalityAndLastSetBit counterCollector = bitmaps.inPlaceAndNotWithCardinalityAndLastSetBit(counter.get(), termIndex);
                        afterCount = counterCollector.cardinality;
                    } else {
                        CardinalityAndLastSetBit<BM> counterCollector = bitmaps.andNotWithCardinalityAndLastSetBit(counter.get(), termIndex);
                        counter = Optional.of(counterCollector.bitmap);
                        afterCount = counterCollector.cardinality;
                    }
                } else {
                    afterCount = answerCollector.cardinality;
                }

                boolean unread = false;
                if (unreadIndex != null) {
                    //TODO much more efficient to add a bitmaps.intersects() to test for first bit in common
                    CardinalityAndLastSetBit<BM> storage = bitmaps.andWithCardinalityAndLastSetBit(Arrays.asList(unreadIndex, termIndex));
                    if (storage.cardinality > 0) {
                        unread = true;
                    }
                }

                aggregateCounts.add(new AggregateCount(aggregateTerm, null, beforeCount - afterCount, -1L, unread));
                beforeCount = afterCount;
            }

            while (true) {
                int lastSetBit = answerCollector == null ? bitmaps.lastSetBit(answer) : answerCollector.lastSetBit;
                log.trace("lastSetBit={}", lastSetBit);
                if (lastSetBit < 0) {
                    break;
                }

                MiruTermId[] fieldValues = requestContext.getActivityIndex().get(name, lastSetBit, fieldId, stackBuffer);
                if (log.isTraceEnabled()) {
                    log.trace("fieldValues={}", Arrays.toString(fieldValues));
                }
                if (fieldValues == null || fieldValues.length == 0) {
                    if (bitmaps.supportsInPlace()) {
                        BM removeUnknownField = bitmaps.createWithBits(lastSetBit);
                        answerCollector = bitmaps.inPlaceAndNotWithCardinalityAndLastSetBit(answer, removeUnknownField);
                    } else {
                        BM removeUnknownField = bitmaps.createWithBits(lastSetBit);
                        answerCollector = bitmaps.andNotWithCardinalityAndLastSetBit(answer, removeUnknownField);
                        answer = answerCollector.bitmap;
                    }
                    beforeCount--;

                } else {
                    MiruTermId aggregateTermId = fieldValues[0]; // Kinda lame but for now we don't see a need for multi field aggregation.
                    MiruValue aggregateValue = new MiruValue(termComposer.decompose(schema, fieldDefinition, stackBuffer, aggregateTermId));
                    aggregated.add(aggregateValue);

                    Optional<BM> optionalTermIndex = fieldIndex.get(name, fieldId, aggregateTermId).getIndex(stackBuffer);
                    checkState(optionalTermIndex.isPresent(), "Unable to load inverted index for aggregateTermId: " + aggregateTermId);

                    IBM termIndex = optionalTermIndex.get();

                    if (bitmaps.supportsInPlace()) {
                        answerCollector = bitmaps.inPlaceAndNotWithCardinalityAndLastSetBit(answer, termIndex);
                    } else {
                        answerCollector = bitmaps.andNotWithCardinalityAndLastSetBit(answer, termIndex);
                        answer = answerCollector.bitmap;
                    }

                    long afterCount;
                    if (counter.isPresent()) {
                        if (bitmaps.supportsInPlace()) {
                            CardinalityAndLastSetBit<BM> counterCollector = bitmaps.inPlaceAndNotWithCardinalityAndLastSetBit(counter.get(), termIndex);
                            afterCount = counterCollector.cardinality;
                        } else {
                            CardinalityAndLastSetBit<BM> counterCollector = bitmaps.andNotWithCardinalityAndLastSetBit(counter.get(), termIndex);
                            counter = Optional.of(counterCollector.bitmap);
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
                            CardinalityAndLastSetBit<BM> storage = bitmaps.andWithCardinalityAndLastSetBit(Arrays.asList(unreadIndex, termIndex));
                            if (storage.cardinality > 0) {
                                unread = true;
                            }
                        }

                        //TODO much more efficient to accumulate lastSetBits and gather these once at the end
                        MiruValue[][] gatherValues = new MiruValue[gatherFieldIds.length][];
                        for (int i = 0; i < gatherFieldIds.length; i++) {
                            MiruTermId[] termIds = requestContext.getActivityIndex().get(name, lastSetBit, gatherFieldIds[i], stackBuffer);
                            MiruValue[] gather = new MiruValue[termIds.length];
                            for (int j = 0; j < gather.length; j++) {
                                gather[j] = new MiruValue(termComposer.decompose(schema,
                                    schema.getFieldDefinition(gatherFieldIds[i]),
                                    stackBuffer,
                                    termIds[j]));
                            }
                            gatherValues[i] = gather;
                        }
                        //TODO much more efficient to accumulate lastSetBits and gather these once at the end
                        TimeVersionRealtime tvr = requestContext.getActivityIndex().getTimeVersionRealtime(name, lastSetBit, stackBuffer);

                        AggregateCount aggregateCount = new AggregateCount(
                            aggregateValue,
                            gatherValues,
                            beforeCount - afterCount,
                            tvr.timestamp,
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
        return new AggregateCountsAnswerConstraint(aggregateCounts, aggregated, skippedDistincts, collectedDistincts);
    }

}
