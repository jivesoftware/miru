package com.jivesoftware.os.miru.anomaly.plugins;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.filer.io.api.KeyRange;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.anomaly.plugins.AnomalyAnswer.Waveform;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFieldFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmapsDebug;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.BitmapAndLastId;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.plugin.index.MiruTermComposer;
import com.jivesoftware.os.miru.plugin.index.MiruTimeIndex;
import com.jivesoftware.os.miru.plugin.solution.MiruAggregateUtil;
import com.jivesoftware.os.miru.plugin.solution.MiruPartitionResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruRemotePartition;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruRequestHandle;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.plugin.solution.Question;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 *
 */
public class AnomalyQuestion implements Question<AnomalyQuery, AnomalyAnswer, AnomalyReport> {

    private final Anomaly anomaly;
    private final MiruRequest<AnomalyQuery> request;
    private final MiruRemotePartition<AnomalyQuery, AnomalyAnswer, AnomalyReport> remotePartition;
    private final MiruBitmapsDebug bitmapsDebug = new MiruBitmapsDebug();
    private final MiruAggregateUtil aggregateUtil = new MiruAggregateUtil();

    public AnomalyQuestion(Anomaly anomaly,
        MiruRequest<AnomalyQuery> request,
        MiruRemotePartition<AnomalyQuery, AnomalyAnswer, AnomalyReport> remotePartition) {
        this.anomaly = anomaly;
        this.request = request;
        this.remotePartition = remotePartition;
    }

    @Override
    public <BM extends IBM, IBM> MiruPartitionResponse<AnomalyAnswer> askLocal(MiruRequestHandle<BM, IBM, ?> handle,
        Optional<AnomalyReport> report) throws Exception {

        StackBuffer stackBuffer = new StackBuffer();
        MiruSolutionLog solutionLog = new MiruSolutionLog(request.logLevel);
        MiruRequestContext<BM, IBM, ?> context = handle.getRequestContext();
        MiruBitmaps<BM, IBM> bitmaps = handle.getBitmaps();
        MiruSchema schema = context.getSchema();

        // Start building up list of bitmap operations to run
        List<IBM> ands = new ArrayList<>();

        MiruTimeRange timeRange = request.query.timeRange;

        // Short-circuit if the time range doesn't live here
        boolean resultsExhausted = request.query.timeRange.smallestTimestamp > context.getTimeIndex().getLargestTimestamp();
        if (!context.getTimeIndex().intersects(timeRange)) {
            solutionLog.log(MiruSolutionLogLevel.WARN, "No time index intersection. Partition {}: {} doesn't intersect with {}",
                handle.getCoord().partitionId, context.getTimeIndex(), timeRange);
            return new MiruPartitionResponse<>(
                new AnomalyAnswer(
                    Maps.transformValues(request.query.filters,
                        input -> new AnomalyAnswer.Waveform(new long[request.query.divideTimeRangeIntoNSegments])),
                    resultsExhausted),
                solutionLog.asList());
        }

        int lastId = context.getActivityIndex().lastId(stackBuffer);

        long start = System.currentTimeMillis();
        ands.add(bitmaps.buildTimeRangeMask(context.getTimeIndex(), timeRange.smallestTimestamp, timeRange.largestTimestamp, stackBuffer));
        solutionLog.log(MiruSolutionLogLevel.INFO, "anomaly timeRangeMask: {} millis.", System.currentTimeMillis() - start);

        // 1) Execute the combined filter above on the given stream, add the bitmap
        if (MiruFilter.NO_FILTER.equals(request.query.constraintsFilter)) {
            solutionLog.log(MiruSolutionLogLevel.INFO, "anomaly filter: no constraints.");
        } else {
            start = System.currentTimeMillis();
            BM filtered = aggregateUtil.filter("anomaly", bitmaps, context, request.query.constraintsFilter, solutionLog, null, lastId, -1, -1, stackBuffer);
            solutionLog.log(MiruSolutionLogLevel.INFO, "anomaly filter: {} millis.", System.currentTimeMillis() - start);
            ands.add(filtered);
        }

        // 2) Add in the authz check if we have it
        if (!MiruAuthzExpression.NOT_PROVIDED.equals(request.authzExpression)) {
            ands.add(context.getAuthzIndex().getCompositeAuthz(request.authzExpression, stackBuffer));
        }

        // 3) Mask out anything that hasn't made it into the activityIndex yet, or that has been removed from the index
        start = System.currentTimeMillis();
        ands.add(bitmaps.buildIndexMask(lastId, context.getRemovalIndex(), null, stackBuffer));
        solutionLog.log(MiruSolutionLogLevel.INFO, "anomaly indexMask: {} millis.", System.currentTimeMillis() - start);

        // AND it all together to get the final constraints
        bitmapsDebug.debug(solutionLog, bitmaps, "ands", ands);
        start = System.currentTimeMillis();
        BM constrained = bitmaps.and(ands);
        solutionLog.log(MiruSolutionLogLevel.INFO, "anomaly constrained: {} millis.", System.currentTimeMillis() - start);

        if (solutionLog.isLogLevelEnabled(MiruSolutionLogLevel.INFO)) {
            solutionLog.log(MiruSolutionLogLevel.INFO, "anomaly constrained {} items.", bitmaps.cardinality(constrained));
        }

        MiruTimeIndex timeIndex = context.getTimeIndex();
        long currentTime = timeRange.smallestTimestamp;
        long segmentDuration = (timeRange.largestTimestamp - timeRange.smallestTimestamp) / request.query.divideTimeRangeIntoNSegments;
        if (segmentDuration < 1) {
            throw new RuntimeException("Time range is insufficient to be divided into " + request.query.divideTimeRangeIntoNSegments + " segments");
        }

        int[] indexes = new int[request.query.divideTimeRangeIntoNSegments + 1];
        for (int i = 0; i < indexes.length; i++) {
            int closestId = timeIndex.getClosestId(currentTime, stackBuffer);
            if (closestId < 0) {
                closestId = -(closestId + 1); // handle negative "theoretical insertion" index
            }
            indexes[i] = closestId;
            currentTime += segmentDuration;
        }

        int fieldId = schema.getFieldId(request.query.expansionField);
        MiruFieldDefinition fieldDefinition = schema.getFieldDefinition(fieldId);

        MiruFieldIndex<BM, IBM> fieldIndex = context.getFieldIndexProvider().getFieldIndex(MiruFieldType.primary);
        final Map<String, MiruFilter> expandable = new LinkedHashMap<>();
        for (String expansion : request.query.expansionValues) {
            if (expansion.endsWith("*")) {
                KeyRange keyRange = null;
                String baseTerm = expansion.substring(0, expansion.length() - 1);
                if (baseTerm.length() > 0) {
                    MiruTermComposer termComposer = context.getTermComposer();
                    byte[] lowerInclusive = termComposer.prefixLowerInclusive(schema, fieldDefinition, stackBuffer, baseTerm);
                    byte[] upperExclusive = termComposer.prefixUpperExclusive(schema, fieldDefinition, stackBuffer, baseTerm);
                    keyRange = new KeyRange(lowerInclusive, upperExclusive);
                }
                fieldIndex.streamTermIdsForField("anomaly", fieldId, (keyRange == null ? null : Arrays.asList(keyRange)), termId -> {
                    for (Entry<String, MiruFilter> entry : request.query.filters.entrySet()) {
                        ArrayList<MiruFieldFilter> join = new ArrayList<>();
                        join.addAll(entry.getValue().fieldFilters);
                        join.add(MiruFieldFilter.ofTerms(MiruFieldType.primary,
                            request.query.expansionField, new String(termId.getBytes(), StandardCharsets.UTF_8)));

                        expandable.put(entry.getKey() + "-" + new String(termId.getBytes(), StandardCharsets.UTF_8),
                            new MiruFilter(entry.getValue().operation, entry.getValue().inclusiveFilter, join, entry.getValue().subFilters));
                    }
                    return true;
                }, stackBuffer);
            } else if (!expansion.isEmpty()) {
                // TODO use got.
                for (Entry<String, MiruFilter> entry : request.query.filters.entrySet()) {

                    ArrayList<MiruFieldFilter> join = new ArrayList<>();
                    join.addAll(entry.getValue().fieldFilters);
                    join.add(MiruFieldFilter.ofTerms(MiruFieldType.primary, request.query.expansionField, expansion));

                    expandable.put(entry.getKey() + "-" + expansion,
                        new MiruFilter(entry.getValue().operation, entry.getValue().inclusiveFilter, join, entry.getValue().subFilters));
                }
            }
        }

        Map<String, MiruFilter> expand = expandable;
        if (expand.isEmpty()) {
            expand = request.query.filters;
        }

        MiruFieldIndex<BM, IBM> primaryFieldIndex = context.getFieldIndexProvider().getFieldIndex(MiruFieldType.primary);
        int powerBitsFieldId = schema.getFieldId(request.query.powerBitsFieldName);
        MiruFieldDefinition powerBitsFieldDefinition = schema.getFieldDefinition(powerBitsFieldId);
        List<BitmapAndLastId<BM>> powerBitIndexes = new ArrayList<>();

        for (int i = 0; i < 64; i++) {
            powerBitIndexes.add(fetchBits(context, schema, powerBitsFieldDefinition, stackBuffer, String.valueOf(i), primaryFieldIndex, powerBitsFieldId));
        }

        BitmapAndLastId<BM> positive = fetchBits(context, schema, powerBitsFieldDefinition, stackBuffer, String.valueOf("+"), primaryFieldIndex,
            powerBitsFieldId);
        BitmapAndLastId<BM> negative = fetchBits(context, schema, powerBitsFieldDefinition, stackBuffer, String.valueOf("-"), primaryFieldIndex,
            powerBitsFieldId);

        Map<String, AnomalyAnswer.Waveform> waveforms = Maps.newHashMapWithExpectedSize(expand.size());
        start = System.currentTimeMillis();
        int producedWaveformCount = 0;
        for (Map.Entry<String, MiruFilter> entry : expand.entrySet()) {
            AnomalyAnswer.Waveform waveform = null;
            if (!bitmaps.isEmpty(constrained)) {
                BM waveformFiltered = aggregateUtil.filter("anomaly", bitmaps, context, entry.getValue(), solutionLog, null, lastId, -1, -1, stackBuffer);

                BM rawAnswer = bitmaps.and(Arrays.asList(constrained, waveformFiltered));
                if (!bitmaps.isEmpty(rawAnswer)) {
                    long[] mergedWaveform = new long[indexes.length - 1];

                    if (positive.isSet()) {
                        BM positiveAnswer = bitmaps.and(Arrays.asList(positive.getBitmap(), rawAnswer));
                        Waveform positiveWaveform = getWaveform(solutionLog, bitmaps, indexes, powerBitIndexes, entry, rawAnswer, positiveAnswer);
                        for (int i = 0; i < mergedWaveform.length; i++) {
                            mergedWaveform[i] += positiveWaveform.waveform[i];
                        }
                    }

                    if (negative.isSet()) {
                        BM negativeAnswer = bitmaps.and(Arrays.asList(negative.getBitmap(), rawAnswer));
                        Waveform negativeWaveform = getWaveform(solutionLog, bitmaps, indexes, powerBitIndexes, entry, rawAnswer, negativeAnswer);
                        for (int i = 0; i < mergedWaveform.length; i++) {
                            mergedWaveform[i] -= negativeWaveform.waveform[i];
                        }
                    }

                    waveform = new Waveform(mergedWaveform);
                } else {
                    solutionLog.log(MiruSolutionLogLevel.DEBUG, "anomaly empty answer.");
                }
            }
            if (waveform != null) {
                waveforms.put(entry.getKey(), waveform);
                producedWaveformCount++;
                if (producedWaveformCount > 100) { // TODO add to query?
                    solutionLog.log(MiruSolutionLogLevel.INFO, "truncated result to 100");
                    break;
                }
            }
        }
        solutionLog.log(MiruSolutionLogLevel.INFO, "anomaly answered: {} millis.", System.currentTimeMillis() - start);
        solutionLog.log(MiruSolutionLogLevel.INFO, "anomaly answered: {} iterations.", request.query.filters.size());

        AnomalyAnswer result = new AnomalyAnswer(waveforms, resultsExhausted);

        return new MiruPartitionResponse<>(result, solutionLog.asList());
    }

    private <BM extends IBM, IBM> Waveform getWaveform(MiruSolutionLog solutionLog,
        MiruBitmaps<BM, IBM> bitmaps,
        int[] indexes,
        List<BitmapAndLastId<BM>> powerBitIndexes,
        Entry<String, MiruFilter> entry,
        BM rawAnswer,
        BM signedAnswer) throws Exception {

        List<BM> answers = Lists.newArrayList();
        for (int i = 0; i < 64; i++) {
            BitmapAndLastId<BM> powerBitIndex = powerBitIndexes.get(i);
            if (powerBitIndex.isSet()) {
                BM answer = bitmaps.and(Arrays.asList(powerBitIndex.getBitmap(), signedAnswer));
                answers.add(answer);
            } else {
                answers.add(null);
            }
        }

        Waveform waveform = anomaly.metricingAvg(bitmaps, rawAnswer, answers, indexes, 64);
        if (solutionLog.isLogLevelEnabled(MiruSolutionLogLevel.DEBUG)) {
            int cardinality = 0;
            for (int i = 0; i < 64; i++) {
                BM answer = answers.get(i);
                if (answer != null) {
                    cardinality += bitmaps.cardinality(answer);
                }
            }
            solutionLog.log(MiruSolutionLogLevel.DEBUG, "anomaly answer: {} items.", cardinality);
            solutionLog.log(MiruSolutionLogLevel.DEBUG, "anomaly name: {}, waveform: {}.", entry.getKey(), Arrays.toString(waveform.waveform));
        }
        return waveform;
    }

    private <BM extends IBM, IBM> BitmapAndLastId<BM> fetchBits(
        MiruRequestContext<BM, IBM, ?> context,
        MiruSchema schema,
        MiruFieldDefinition powerBitsFieldDefinition,
        StackBuffer stackBuffer,
        String bit,
        MiruFieldIndex<BM, IBM> primaryFieldIndex,
        int powerBitsFieldId) throws Exception {

        MiruTermId powerBitTerm = context.getTermComposer().compose(schema, powerBitsFieldDefinition, stackBuffer, bit);
        MiruInvertedIndex<BM, IBM> invertedIndex = primaryFieldIndex.get("anomaly", powerBitsFieldId, powerBitTerm);
        BitmapAndLastId<BM> container = new BitmapAndLastId<>();
        invertedIndex.getIndex(container, stackBuffer);
        return container;
    }

    @Override
    public MiruPartitionResponse<AnomalyAnswer> askRemote(MiruHost host,
        MiruPartitionId partitionId,
        Optional<AnomalyReport> report) throws MiruQueryServiceException {
        return remotePartition.askRemote(host, partitionId, request, report);
    }

    @Override
    public Optional<AnomalyReport> createReport(Optional<AnomalyAnswer> answer) {
        Optional<AnomalyReport> report = Optional.absent();
        if (answer.isPresent()) {
            report = Optional.of(new AnomalyReport());
        }
        return report;
    }
}
