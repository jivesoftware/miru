package com.jivesoftware.os.miru.stumptown.plugins;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmapsDebug;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class StumptownQuestion implements Question<StumptownQuery, StumptownAnswer, StumptownReport> {

    private final Stumptown stumptown;
    private final MiruRequest<StumptownQuery> request;
    private final MiruRemotePartition<StumptownQuery, StumptownAnswer, StumptownReport> remotePartition;
    private final MiruBitmapsDebug bitmapsDebug = new MiruBitmapsDebug();
    private final MiruAggregateUtil aggregateUtil = new MiruAggregateUtil();

    public StumptownQuestion(Stumptown stumptown,
        MiruRequest<StumptownQuery> request,
        MiruRemotePartition<StumptownQuery, StumptownAnswer, StumptownReport> remotePartition) {
        this.stumptown = stumptown;
        this.request = request;
        this.remotePartition = remotePartition;
    }

    @Override
    public <BM extends IBM, IBM> MiruPartitionResponse<StumptownAnswer> askLocal(MiruRequestHandle<BM, IBM, ?> handle,
        Optional<StumptownReport> report) throws Exception {

        StackBuffer stackBuffer = new StackBuffer();

        MiruSolutionLog solutionLog = new MiruSolutionLog(request.logLevel);
        MiruRequestContext<BM, IBM, ?> context = handle.getRequestContext();
        MiruBitmaps<BM, IBM> bitmaps = handle.getBitmaps();

        // Start building up list of bitmap operations to run
        List<IBM> ands = new ArrayList<>();

        MiruTimeRange timeRange = request.query.timeRange;

        // Short-circuit if the time range doesn't live here
        boolean resultsExhausted = request.query.timeRange.smallestTimestamp > context.getTimeIndex().getLargestTimestamp();
        if (!context.getTimeIndex().intersects(timeRange)) {
            solutionLog.log(MiruSolutionLogLevel.WARN, "No time index intersection. Partition {}: {} doesn't intersect with {}",
                handle.getCoord().partitionId, context.getTimeIndex(), timeRange);
            return new MiruPartitionResponse<>(
                new StumptownAnswer(
                    Maps.transformValues(request.query.stumptownFilters,
                        input -> new StumptownAnswer.Waveform(new long[request.query.divideTimeRangeIntoNSegments],
                            Collections.<MiruActivity>emptyList())),
                    resultsExhausted),
                solutionLog.asList());
        }

        int lastId = context.getActivityIndex().lastId(stackBuffer);

        long start = System.currentTimeMillis();
        ands.add(bitmaps.buildTimeRangeMask(context.getTimeIndex(), timeRange.smallestTimestamp, timeRange.largestTimestamp, stackBuffer));
        solutionLog.log(MiruSolutionLogLevel.INFO, "stumptown timeRangeMask: {} millis.", System.currentTimeMillis() - start);

        // 1) Execute the combined filter above on the given stream, add the bitmap
        if (MiruFilter.NO_FILTER.equals(request.query.constraintsFilter)) {
            solutionLog.log(MiruSolutionLogLevel.INFO, "stumptown filter: no constraints.");
        } else {
            start = System.currentTimeMillis();
            BM filtered = aggregateUtil.filter("stumptown", bitmaps, context, request.query.constraintsFilter, solutionLog, null, lastId, -1, stackBuffer);
            solutionLog.log(MiruSolutionLogLevel.INFO, "stumptown filter: {} millis.", System.currentTimeMillis() - start);
            ands.add(filtered);
        }

        // 2) Add in the authz check if we have it
        if (!MiruAuthzExpression.NOT_PROVIDED.equals(request.authzExpression)) {
            ands.add(context.getAuthzIndex().getCompositeAuthz(request.authzExpression, stackBuffer));
        }

        // 3) Mask out anything that hasn't made it into the activityIndex yet, or that has been removed from the index
        start = System.currentTimeMillis();
        ands.add(bitmaps.buildIndexMask(lastId, context.getRemovalIndex(), null, stackBuffer));
        solutionLog.log(MiruSolutionLogLevel.INFO, "stumptown indexMask: {} millis.", System.currentTimeMillis() - start);

        // AND it all together to get the final constraints
        bitmapsDebug.debug(solutionLog, bitmaps, "ands", ands);
        start = System.currentTimeMillis();
        BM constrained = bitmaps.and(ands);
        solutionLog.log(MiruSolutionLogLevel.INFO, "stumptown constrained: {} millis.", System.currentTimeMillis() - start);

        if (solutionLog.isLogLevelEnabled(MiruSolutionLogLevel.INFO)) {
            solutionLog.log(MiruSolutionLogLevel.INFO, "stumptown constrained {} items.", bitmaps.cardinality(constrained));
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

        Map<String, StumptownAnswer.Waveform> waveforms = Maps.newHashMapWithExpectedSize(request.query.stumptownFilters.size());
        start = System.currentTimeMillis();
        for (Map.Entry<String, MiruFilter> entry : request.query.stumptownFilters.entrySet()) {
            StumptownAnswer.Waveform waveform = null;
            if (!bitmaps.isEmpty(constrained)) {
                BM waveformFiltered = aggregateUtil.filter("stumptown", bitmaps, context, entry.getValue(), solutionLog, null, lastId, -1, stackBuffer);
                BM answer = bitmaps.and(Arrays.asList(constrained, waveformFiltered));
                if (!bitmaps.isEmpty(answer)) {
                    waveform = stumptown.stumptowning("stumptown", bitmaps, context, request.tenantId, answer, request.query.desiredNumberOfResultsPerWaveform,
                        indexes);
                    if (solutionLog.isLogLevelEnabled(MiruSolutionLogLevel.DEBUG)) {
                        solutionLog.log(MiruSolutionLogLevel.DEBUG, "stumptown answer: {} items.", bitmaps.cardinality(answer));
                        solutionLog.log(MiruSolutionLogLevel.DEBUG, "stumptown name: {}, waveform: {}.", entry.getKey(), Arrays.toString(waveform.waveform));
                    }
                } else {
                    solutionLog.log(MiruSolutionLogLevel.DEBUG, "stumptown empty answer.");
                }
            }
            if (waveform == null) {
                waveform = new StumptownAnswer.Waveform(new long[request.query.divideTimeRangeIntoNSegments], Collections.<MiruActivity>emptyList());
            }
            waveforms.put(entry.getKey(), waveform);
        }
        solutionLog.log(MiruSolutionLogLevel.INFO, "stumptown answered: {} millis.", System.currentTimeMillis() - start);
        solutionLog.log(MiruSolutionLogLevel.INFO, "stumptown answered: {} iterations.", request.query.stumptownFilters.size());

        StumptownAnswer result = new StumptownAnswer(waveforms, resultsExhausted);

        return new MiruPartitionResponse<>(result, solutionLog.asList());
    }

    @Override
    public MiruPartitionResponse<StumptownAnswer> askRemote(MiruHost host,
        MiruPartitionId partitionId,
        Optional<StumptownReport> report) throws MiruQueryServiceException {
        return remotePartition.askRemote(host, partitionId, request, report);
    }

    @Override
    public Optional<StumptownReport> createReport(Optional<StumptownAnswer> answer) {
        Optional<StumptownReport> report = Optional.absent();
        if (answer.isPresent()) {
            report = Optional.of(new StumptownReport());
        }
        return report;
    }
}
