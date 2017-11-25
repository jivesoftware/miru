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
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.BitmapAndLastId;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruTermComposer;
import com.jivesoftware.os.miru.plugin.index.TimeVersionRealtime;
import com.jivesoftware.os.miru.plugin.solution.MiruAggregateUtil;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;

import java.io.IOException;
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

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruAggregateUtil aggregateUtil = new MiruAggregateUtil();

    public <BM extends IBM, IBM> AggregateCountsAnswer getAggregateCounts(String name,
        MiruSolutionLog solutionLog,
        MiruBitmaps<BM, IBM> bitmaps,
        MiruRequestContext<BM, IBM, ?> requestContext,
        MiruRequest<AggregateCountsQuery> request,
        MiruPartitionCoord coord,
        Optional<AggregateCountsReport> lastReport,
        BM answer,
        Optional<BM> counter,
        boolean verbose)
        throws Exception {
        LOG.trace("Get aggregate counts for answer: {} request: {}", answer, request);

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
                    request.query.collectTimeRange,
                    request.query.includeUnreadState,
                    entry.getValue(),
                    lastReportConstraint,
                    answer,
                    counter,
                    verbose));
        }

        boolean resultsExhausted = request.query.answerTimeRange.smallestTimestamp > requestContext.getTimeIndex().getLargestTimestamp();
        AggregateCountsAnswer result = new AggregateCountsAnswer(results, resultsExhausted);
        LOG.trace("result: {}", result);
        return result;
    }

    private <BM extends IBM, IBM> AggregateCountsAnswerConstraint answerConstraint(String name,
        MiruSolutionLog solutionLog,
        MiruBitmaps<BM, IBM> bitmaps,
        MiruRequestContext<BM, IBM, ?> requestContext,
        MiruPartitionCoord coord,
        MiruStreamId streamId,
        MiruTimeRange collectTimeRange,
        boolean includeUnreadState,
        AggregateCountsQueryConstraint constraint,
        Optional<AggregateCountsReportConstraint> lastReport,
        BM answer,
        Optional<BM> counter,
        boolean verbose) throws Exception {
        StackBuffer stackBuffer = new StackBuffer();

        MiruSchema schema = requestContext.getSchema();
        int fieldId = schema.getFieldId(constraint.aggregateCountAroundField);
        MiruFieldDefinition fieldDefinition = schema.getFieldDefinition(fieldId);
        Preconditions.checkArgument(fieldDefinition.type.hasFeature(Feature.stored), "You can only aggregate stored fields");

        MiruFieldDefinition[] gatherFieldDefinitions;
        if (constraint.gatherTermsForFields != null && constraint.gatherTermsForFields.length > 0) {
            gatherFieldDefinitions = new MiruFieldDefinition[constraint.gatherTermsForFields.length];
            for (int i = 0; i < gatherFieldDefinitions.length; i++) {
                gatherFieldDefinitions[i] = schema.getFieldDefinition(schema.getFieldId(constraint.gatherTermsForFields[i]));
                Preconditions.checkArgument(gatherFieldDefinitions[i].type.hasFeature(Feature.stored),
                    "You can only gather stored fields");
            }
        } else {
            gatherFieldDefinitions = new MiruFieldDefinition[0];
        }

        // don't mutate the original
        answer = bitmaps.copy(answer);

        int collectedDistincts = 0;
        int skippedDistincts = 0;
        Set<MiruValue> aggregated;
        Set<MiruValue> uncollected;
        if (lastReport.isPresent()) {
            collectedDistincts = lastReport.get().collectedDistincts;
            skippedDistincts = lastReport.get().skippedDistincts;
            aggregated = Sets.newHashSet(lastReport.get().aggregateTerms);
            uncollected = Sets.newHashSet(lastReport.get().uncollectedTerms);
            if (verbose) {
                LOG.info("Aggregate counts report coord:{} streamId:{} aggregated:{} uncollected:{}", coord, streamId, aggregated, uncollected);
            }
        } else {
            aggregated = Sets.newHashSet();
            uncollected = Sets.newHashSet();
            if (verbose) {
                LOG.info("Aggregate counts no report coord:{} streamId:{}", coord, streamId);
            }
        }

        if (!MiruFilter.NO_FILTER.equals(constraint.constraintsFilter)) {
            int lastId = requestContext.getActivityIndex().lastId(stackBuffer);
            BM filtered = aggregateUtil.filter(name, bitmaps, requestContext, constraint.constraintsFilter, solutionLog, null, lastId, -1, -1, stackBuffer);

            bitmaps.inPlaceAnd(answer, filtered);

            if (counter.isPresent()) {
                bitmaps.inPlaceAnd(counter.get(), filtered);
            }
        }

        MiruTermComposer termComposer = requestContext.getTermComposer();
        MiruFieldIndex<BM, IBM> fieldIndex = requestContext.getFieldIndexProvider().getFieldIndex(MiruFieldType.primary);
        LOG.trace("fieldId: {}", fieldId);

        List<AggregateCount> aggregateCounts = new ArrayList<>();
        BitmapAndLastId<BM> container = new BitmapAndLastId<>();
        if (fieldId >= 0) {
            IBM unreadAnswer = null;
            if (includeUnreadState && !MiruStreamId.NULL.equals(streamId)) {
                container.clear();
                requestContext.getUnreadTrackingIndex().getUnread(streamId).getIndex(container, stackBuffer);
                if (container.isSet()) {
                    unreadAnswer = bitmaps.and(Arrays.asList(container.getBitmap(), answer));
                }
            }

            long beforeCount = counter.isPresent() ? bitmaps.cardinality(counter.get()) : bitmaps.cardinality(answer);
            for (MiruValue aggregateTerm : aggregated) {
                MiruTermId aggregateTermId = termComposer.compose(schema, fieldDefinition, stackBuffer, aggregateTerm.parts);
                container.clear();
                fieldIndex.get(name, fieldId, aggregateTermId).getIndex(container, stackBuffer);
                if (!container.isSet()) {
                    continue;
                }

                IBM termIndex = container.getBitmap();

                // docs:   aaabbaccaaabbbcbbcbaac
                // unread: 0000000000000000000000
                // query:  1111111111111111111111
                // read a: 0001101100011111111001
                // docs:                         abcabc
                // query:  0001101100011111111001111111
                // read b: 0000001100000010010001101101
                // a?      000  0  000        00 1  1      any=1, oldest=0, latest=1
                // b?         00      000 00 0    0  0     any=0, oldest=0, latest=0
                // c?            11      1  1   1  1  1    any=1, oldest=1, latest=1

                int firstIntersectingBit = bitmaps.firstIntersectingBit(answer, termIndex);

                boolean anyUnread = false;
                boolean oldestUnread = false;
                if (firstIntersectingBit != -1 && unreadAnswer != null) {
                    anyUnread = bitmaps.intersects(unreadAnswer, termIndex);
                    oldestUnread = bitmaps.isSet(unreadAnswer, firstIntersectingBit);
                }

                bitmaps.inPlaceAndNot(answer, termIndex);

                long afterCount;
                if (counter.isPresent()) {
                    bitmaps.inPlaceAndNot(counter.get(), termIndex);
                    afterCount = bitmaps.cardinality(counter.get());
                } else {
                    afterCount = bitmaps.cardinality(answer);
                }

                TimeVersionRealtime oldestTVR;
                MiruValue[][] oldestValues;
                if (firstIntersectingBit != -1) {
                    oldestTVR = requestContext.getActivityIndex().getTimeVersionRealtime(name, firstIntersectingBit, stackBuffer);

                    //TODO much more efficient to accumulate bits and gather these once at the end
                    oldestValues = new MiruValue[gatherFieldDefinitions.length][];
                    for (int i = 0; i < gatherFieldDefinitions.length; i++) {
                        MiruFieldDefinition gatherFieldDefinition = gatherFieldDefinitions[i];
                        MiruTermId[] termIds = requestContext.getActivityIndex().get(name, firstIntersectingBit, gatherFieldDefinition, stackBuffer);
                        oldestValues[i] = termsToValues(stackBuffer, schema, termComposer, gatherFieldDefinition, termIds);
                    }
                    //TODO much more efficient to accumulate bits and gather these once at the end
                } else {
                    oldestTVR = null;
                    oldestValues = null;
                }

                aggregateCounts.add(new AggregateCount(aggregateTerm,
                    null,
                    oldestValues,
                    beforeCount - afterCount,
                    -1L,
                    oldestTVR == null ? -1 : oldestTVR.timestamp,
                    anyUnread,
                    false,
                    oldestUnread));
                beforeCount = afterCount;
            }

            for (MiruValue uncollectedTerm : uncollected) {
                MiruTermId uncollectedTermId = termComposer.compose(schema, fieldDefinition, stackBuffer, uncollectedTerm.parts);
                container.clear();
                fieldIndex.get(name, fieldId, uncollectedTermId).getIndex(container, stackBuffer);
                if (!container.isSet()) {
                    continue;
                }

                IBM termIndex = container.getBitmap();

                bitmaps.inPlaceAndNot(answer, termIndex);

                if (counter.isPresent()) {
                    bitmaps.inPlaceAndNot(counter.get(), termIndex);
                }
            }

            while (true) {
                int lastSetBit = bitmaps.lastSetBit(answer);
                LOG.trace("lastSetBit: {}", lastSetBit);
                if (lastSetBit < 0) {
                    break;
                }

                MiruTermId[] fieldValues = requestContext.getActivityIndex().get(name, lastSetBit, fieldDefinition, stackBuffer);
                if (LOG.isTraceEnabled()) {
                    LOG.trace("fieldValues: {}", Arrays.toString(fieldValues));
                }
                if (fieldValues == null || fieldValues.length == 0) {
                    BM removeUnknownField = bitmaps.createWithBits(lastSetBit);
                    bitmaps.inPlaceAndNot(answer, removeUnknownField);
                    beforeCount--;
                } else {
                    MiruTermId aggregateTermId = fieldValues[0]; // Kinda lame but for now we don't see a need for multi field aggregation.
                    MiruValue aggregateValue = new MiruValue(termComposer.decompose(schema, fieldDefinition, stackBuffer, aggregateTermId));

                    container.clear();
                    fieldIndex.get(name, fieldId, aggregateTermId).getIndex(container, stackBuffer);
                    checkState(container.isSet(), "Unable to load inverted index for aggregateTermId: %s", aggregateTermId);

                    IBM termIndex = container.getBitmap();
                    int firstIntersectingBit = bitmaps.firstIntersectingBit(answer, termIndex);

                    TimeVersionRealtime latestTVR, oldestTVR;
                    if (lastSetBit == firstIntersectingBit) {
                        latestTVR = requestContext.getActivityIndex().getTimeVersionRealtime(name, lastSetBit, stackBuffer);
                        oldestTVR = latestTVR;
                    } else {
                        TimeVersionRealtime[] tvr = requestContext.getActivityIndex().getAllTimeVersionRealtime(name,
                            new int[]{lastSetBit, firstIntersectingBit},
                            stackBuffer);
                        latestTVR = tvr[0];
                        oldestTVR = tvr[1];
                    }

                    boolean collected = contains(collectTimeRange, latestTVR.monoTimestamp);
                    if (collected) {
                        aggregated.add(aggregateValue);
                        collectedDistincts++;
                        if (verbose) {
                            LOG.info("Aggregate counts aggregated coord:{} streamId:{} value:{} timestamp:{} collect:{}",
                                coord, streamId, aggregateValue, latestTVR.monoTimestamp, collectTimeRange);
                        }
                    } else {
                        uncollected.add(aggregateValue);
                        skippedDistincts++;
                        if (verbose) {
                            LOG.info("Aggregate counts uncollected coord:{} streamId:{} value:{} timestamp:{} collect:{}",
                                coord, streamId, aggregateValue, latestTVR.monoTimestamp, collectTimeRange);
                        }
                    }

                    boolean anyUnread = false;
                    boolean latestUnread = false;
                    boolean oldestUnread = false;
                    if (collected && collectedDistincts > constraint.startFromDistinctN && unreadAnswer != null) {
                        anyUnread = bitmaps.intersects(unreadAnswer, termIndex);
                        latestUnread = lastSetBit != -1 && bitmaps.isSet(unreadAnswer, lastSetBit);
                        oldestUnread = firstIntersectingBit != -1 && bitmaps.isSet(unreadAnswer, firstIntersectingBit);
                    }

                    bitmaps.inPlaceAndNot(answer, termIndex);

                    long afterCount;
                    if (counter.isPresent()) {
                        bitmaps.inPlaceAndNot(counter.get(), termIndex);
                        afterCount = bitmaps.cardinality(counter.get());
                    } else {
                        afterCount = bitmaps.cardinality(answer);
                    }

                    if (collected && collectedDistincts > constraint.startFromDistinctN) {
                        //TODO much more efficient to accumulate bits and gather these once at the end
                        MiruValue[][] latestValues = new MiruValue[gatherFieldDefinitions.length][];
                        MiruValue[][] oldestValues = new MiruValue[gatherFieldDefinitions.length][];
                        for (int i = 0; i < gatherFieldDefinitions.length; i++) {
                            MiruFieldDefinition gatherFieldDefinition = gatherFieldDefinitions[i];
                            if (lastSetBit == firstIntersectingBit) {
                                MiruTermId[] termIds = requestContext.getActivityIndex().get(name, lastSetBit, gatherFieldDefinition, stackBuffer);
                                MiruValue[] values = termsToValues(stackBuffer, schema, termComposer, gatherFieldDefinition, termIds);
                                latestValues[i] = values;
                                oldestValues[i] = values;
                            } else {
                                MiruTermId[][] termIds = requestContext.getActivityIndex().getAll(name,
                                    new int[]{lastSetBit, firstIntersectingBit},
                                    gatherFieldDefinition,
                                    stackBuffer);
                                latestValues[i] = termsToValues(stackBuffer, schema, termComposer, gatherFieldDefinition, termIds[0]);
                                oldestValues[i] = termsToValues(stackBuffer, schema, termComposer, gatherFieldDefinition, termIds[1]);
                            }
                        }
                        //TODO much more efficient to accumulate bits and gather these once at the end

                        AggregateCount aggregateCount = new AggregateCount(aggregateValue,
                            latestValues,
                            oldestValues,
                            beforeCount - afterCount,
                            latestTVR.timestamp,
                            oldestTVR.timestamp,
                            anyUnread,
                            latestUnread,
                            oldestUnread);
                        aggregateCounts.add(aggregateCount);

                        if (aggregateCounts.size() >= constraint.desiredNumberOfDistincts) {
                            break;
                        }
                    }
                    beforeCount = afterCount;
                }
            }
        }
        return new AggregateCountsAnswerConstraint(aggregateCounts, aggregated, uncollected, skippedDistincts, collectedDistincts);
    }

    private MiruValue[] termsToValues(StackBuffer stackBuffer,
        MiruSchema schema,
        MiruTermComposer termComposer,
        MiruFieldDefinition gatherFieldDefinition,
        MiruTermId[] termIds) throws IOException {
        MiruValue[] values = new MiruValue[termIds.length];
        for (int j = 0; j < values.length; j++) {
            values[j] = new MiruValue(termComposer.decompose(schema,
                gatherFieldDefinition,
                stackBuffer,
                termIds[j]));
        }
        return values;
    }

    private static boolean contains(MiruTimeRange timeRange, long timestamp) {
        return timeRange.smallestTimestamp <= timestamp && timeRange.largestTimestamp >= timestamp;
    }

}
