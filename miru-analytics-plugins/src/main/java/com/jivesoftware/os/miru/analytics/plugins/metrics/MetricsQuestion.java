package com.jivesoftware.os.miru.analytics.plugins.metrics;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruValue;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmapsDebug;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.BitmapAndLastId;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
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
import com.jivesoftware.os.miru.plugin.solution.Waveform;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class MetricsQuestion implements Question<MetricsQuery, MetricsAnswer, MetricsReport> {

    private final Metrics metrics;
    private final MiruRequest<MetricsQuery> request;
    private final MiruRemotePartition<MetricsQuery, MetricsAnswer, MetricsReport> remotePartition;
    private final MiruBitmapsDebug bitmapsDebug = new MiruBitmapsDebug();
    private final MiruAggregateUtil aggregateUtil = new MiruAggregateUtil();

    public MetricsQuestion(Metrics metrics,
        MiruRequest<MetricsQuery> request,
        MiruRemotePartition<MetricsQuery, MetricsAnswer, MetricsReport> remotePartition) {
        this.metrics = metrics;
        this.request = request;
        this.remotePartition = remotePartition;
    }

    @Override
    public <BM extends IBM, IBM> MiruPartitionResponse<MetricsAnswer> askLocal(MiruRequestHandle<BM, IBM, ?> handle,
        Optional<MetricsReport> report) throws Exception {

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

            Set<String> keys = request.query.filters.keySet();
            List<Waveform> waveforms = Lists.newArrayListWithCapacity(keys.size());
            for (String key : keys) {
                waveforms.add(Waveform.empty(new MiruValue(key), request.query.divideTimeRangeIntoNSegments));
            }
            return new MiruPartitionResponse<>(new MetricsAnswer(waveforms, resultsExhausted), solutionLog.asList());
        }

        int lastId = context.getActivityIndex().lastId(stackBuffer);

        long start = System.currentTimeMillis();
        ands.add(bitmaps.buildTimeRangeMask(context.getTimeIndex(), timeRange.smallestTimestamp, timeRange.largestTimestamp, stackBuffer));
        solutionLog.log(MiruSolutionLogLevel.INFO, "metrics timeRangeMask: {} millis.", System.currentTimeMillis() - start);

        // 1) Execute the combined filter above on the given stream, add the bitmap
        if (MiruFilter.NO_FILTER.equals(request.query.constraintsFilter)) {
            solutionLog.log(MiruSolutionLogLevel.INFO, "metrics filter: no constraints.");
        } else {
            start = System.currentTimeMillis();
            BM filtered = aggregateUtil.filter("metrics", bitmaps, context, request.query.constraintsFilter, solutionLog, null, lastId, -1, -1, stackBuffer);
            solutionLog.log(MiruSolutionLogLevel.INFO, "metrics filter: {} millis.", System.currentTimeMillis() - start);
            ands.add(filtered);
        }

        // 2) Add in the authz check if we have it
        if (!MiruAuthzExpression.NOT_PROVIDED.equals(request.authzExpression)) {
            ands.add(context.getAuthzIndex().getCompositeAuthz(request.authzExpression, stackBuffer));
        }

        // 3) Mask out anything that hasn't made it into the activityIndex yet, or that has been removed from the index
        start = System.currentTimeMillis();
        ands.add(bitmaps.buildIndexMask(lastId, context.getRemovalIndex(), null, stackBuffer));
        solutionLog.log(MiruSolutionLogLevel.INFO, "metrics indexMask: {} millis.", System.currentTimeMillis() - start);

        // AND it all together to get the final constraints
        bitmapsDebug.debug(solutionLog, bitmaps, "ands", ands);
        start = System.currentTimeMillis();
        BM constrained = bitmaps.and(ands);
        solutionLog.log(MiruSolutionLogLevel.INFO, "metrics constrained: {} millis.", System.currentTimeMillis() - start);

        if (solutionLog.isLogLevelEnabled(MiruSolutionLogLevel.INFO)) {
            solutionLog.log(MiruSolutionLogLevel.INFO, "metrics constrained {} items.", bitmaps.cardinality(constrained));
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
            indexes[i] = timeIndex.getClosestId(currentTime, stackBuffer);
            currentTime += segmentDuration;
        }

        MiruFieldIndex<BM, IBM> primaryFieldIndex = context.getFieldIndexProvider().getFieldIndex(MiruFieldType.primary);
        int powerBitsFieldId = context.getSchema().getFieldId(request.query.powerBitsFieldName);
        MiruFieldDefinition powerBitsFieldDefinition = context.getSchema().getFieldDefinition(powerBitsFieldId);
        List<BitmapAndLastId<BM>> powerBitIndexes = new ArrayList<>();
        for (int i = 0; i < 64; i++) {
            MiruTermId powerBitTerm = context.getTermComposer().compose(schema, powerBitsFieldDefinition, stackBuffer, String.valueOf(i));
            MiruInvertedIndex<BM, IBM> invertedIndex = primaryFieldIndex.get("metrics", powerBitsFieldId, powerBitTerm);
            BitmapAndLastId<BM> powerBitContainer = new BitmapAndLastId<>();
            invertedIndex.getIndex(powerBitContainer, stackBuffer);
            powerBitIndexes.add(powerBitContainer);
        }

        List<Waveform> waveforms = Lists.newArrayListWithCapacity(request.query.filters.size());
        start = System.currentTimeMillis();
        for (Map.Entry<String, MiruFilter> entry : request.query.filters.entrySet()) {
            Waveform waveform = null;
            if (!bitmaps.isEmpty(constrained)) {
                BM waveformFiltered = aggregateUtil.filter("metrics", bitmaps, context, entry.getValue(), solutionLog, null, lastId, -1, -1, stackBuffer);

                BM rawAnswer = bitmaps.and(Arrays.asList(constrained, waveformFiltered));
                if (!bitmaps.isEmpty(rawAnswer)) {
                    List<BM> answers = Lists.newArrayList();
                    for (int i = 0; i < 64; i++) {
                        BitmapAndLastId<BM> powerBitIndex = powerBitIndexes.get(i);
                        if (powerBitIndex.isSet()) {
                            BM answer = bitmaps.and(Arrays.asList(powerBitIndex.getBitmap(), rawAnswer));
                            answers.add(answer);
                        } else {
                            answers.add(null);
                        }
                    }

                    waveform = metrics.metricingAvg(entry.getKey(), bitmaps, rawAnswer, answers, indexes, 64);
                    if (solutionLog.isLogLevelEnabled(MiruSolutionLogLevel.DEBUG)) {
                        int cardinality = 0;
                        for (int i = 0; i < 64; i++) {
                            BM answer = answers.get(i);
                            if (answer != null) {
                                cardinality += bitmaps.cardinality(answer);
                            }
                        }
                        solutionLog.log(MiruSolutionLogLevel.DEBUG, "metrics answer: {} items.", cardinality);
                        solutionLog.log(MiruSolutionLogLevel.DEBUG, "metrics name: {}, waveform: {}.", entry.getKey(), waveform);
                    }
                } else {
                    solutionLog.log(MiruSolutionLogLevel.DEBUG, "metrics empty answer.");
                }
            }
            if (waveform == null) {
                waveform = Waveform.empty(new MiruValue(entry.getKey()), request.query.divideTimeRangeIntoNSegments);
            }
            waveforms.add(waveform);
        }
        solutionLog.log(MiruSolutionLogLevel.INFO, "metrics answered: {} millis.", System.currentTimeMillis() - start);
        solutionLog.log(MiruSolutionLogLevel.INFO, "metrics answered: {} iterations.", request.query.filters.size());

        MetricsAnswer result = new MetricsAnswer(waveforms, resultsExhausted);

        return new MiruPartitionResponse<>(result, solutionLog.asList());
    }

    @Override
    public MiruPartitionResponse<MetricsAnswer> askRemote(MiruHost host,
        MiruPartitionId partitionId,
        Optional<MetricsReport> report) throws MiruQueryServiceException {
        return remotePartition.askRemote(host, partitionId, request, report);
    }

    @Override
    public Optional<MetricsReport> createReport(Optional<MetricsAnswer> answer) {
        Optional<MetricsReport> report = Optional.absent();
        if (answer.isPresent()) {
            report = Optional.of(new MetricsReport());
        }
        return report;
    }
}
