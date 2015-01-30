package com.jivesoftware.os.miru.lumberyard.plugins;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
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
public class LumberyardQuestion implements Question<LumberyardAnswer, LumberyardReport> {

    private final Lumberyard lumberyard;
    private final MiruRequest<LumberyardQuery> request;
    private final MiruBitmapsDebug bitmapsDebug = new MiruBitmapsDebug();
    private final MiruAggregateUtil aggregateUtil = new MiruAggregateUtil();

    public LumberyardQuestion(Lumberyard lumberyard,
        MiruRequest<LumberyardQuery> request) {
        this.lumberyard = lumberyard;
        this.request = request;
    }

    @Override
    public <BM> MiruPartitionResponse<LumberyardAnswer> askLocal(MiruRequestHandle<BM> handle, Optional<LumberyardReport> report) throws Exception {
        MiruSolutionLog solutionLog = new MiruSolutionLog(request.logLevel);
        MiruRequestContext<BM> context = handle.getRequestContext();
        MiruBitmaps<BM> bitmaps = handle.getBitmaps();

        // Start building up list of bitmap operations to run
        List<BM> ands = new ArrayList<>();

        MiruTimeRange timeRange = request.query.timeRange;

        // Short-circuit if the time range doesn't live here
        boolean resultsExhausted = request.query.timeRange.smallestTimestamp > context.getTimeIndex().getLargestTimestamp();
        if (!timeIndexIntersectsTimeRange(context.getTimeIndex(), timeRange)) {
            solutionLog.log(MiruSolutionLogLevel.WARN, "No time index intersection");
            return new MiruPartitionResponse<>(
                new LumberyardAnswer(
                    Maps.transformValues(request.query.lumberyardFilters,
                        new Function<MiruFilter, LumberyardAnswer.Waveform>() {
                            @Override
                            public LumberyardAnswer.Waveform apply(MiruFilter input) {
                                return new LumberyardAnswer.Waveform(new long[request.query.divideTimeRangeIntoNSegments],
                                    Collections.<MiruActivity>emptyList());
                            }
                        }),
                    resultsExhausted),
                solutionLog.asList());
        }

        long start = System.currentTimeMillis();
        ands.add(bitmaps.buildTimeRangeMask(context.getTimeIndex(), timeRange.smallestTimestamp, timeRange.largestTimestamp));
        solutionLog.log(MiruSolutionLogLevel.INFO, "lumberyard timeRangeMask: {} millis.", System.currentTimeMillis() - start);

        // 1) Execute the combined filter above on the given stream, add the bitmap
        if (MiruFilter.NO_FILTER.equals(request.query.constraintsFilter)) {
            solutionLog.log(MiruSolutionLogLevel.INFO, "lumberyard filter: no constraints.");
        } else {
            BM filtered = bitmaps.create();
            start = System.currentTimeMillis();
            aggregateUtil.filter(bitmaps, context.getSchema(), context.getTermComposer(), context.getFieldIndexProvider(), request.query.constraintsFilter,
                solutionLog, filtered, context.getActivityIndex().lastId(), -1);
            solutionLog.log(MiruSolutionLogLevel.INFO, "lumberyard filter: {} millis.", System.currentTimeMillis() - start);
            ands.add(filtered);
        }

        // 2) Add in the authz check if we have it
        if (!MiruAuthzExpression.NOT_PROVIDED.equals(request.authzExpression)) {
            ands.add(context.getAuthzIndex().getCompositeAuthz(request.authzExpression));
        }

        // 3) Mask out anything that hasn't made it into the activityIndex yet, or that has been removed from the index
        start = System.currentTimeMillis();
        ands.add(bitmaps.buildIndexMask(context.getActivityIndex().lastId(), context.getRemovalIndex().getIndex()));
        solutionLog.log(MiruSolutionLogLevel.INFO, "lumberyard indexMask: {} millis.", System.currentTimeMillis() - start);

        // AND it all together to get the final constraints
        BM constrained = bitmaps.create();
        bitmapsDebug.debug(solutionLog, bitmaps, "ands", ands);
        start = System.currentTimeMillis();
        bitmaps.and(constrained, ands);
        solutionLog.log(MiruSolutionLogLevel.INFO, "lumberyard constrained: {} millis.", System.currentTimeMillis() - start);

        if (solutionLog.isLogLevelEnabled(MiruSolutionLogLevel.INFO)) {
            solutionLog.log(MiruSolutionLogLevel.INFO, "lumberyard constrained {} items.", bitmaps.cardinality(constrained));
        }

        MiruTimeIndex timeIndex = context.getTimeIndex();
        long currentTime = timeRange.smallestTimestamp;
        long segmentDuration = (timeRange.largestTimestamp - timeRange.smallestTimestamp) / request.query.divideTimeRangeIntoNSegments;
        if (segmentDuration < 1) {
            throw new RuntimeException("Time range is insufficient to be divided into " + request.query.divideTimeRangeIntoNSegments + " segments");
        }

        int[] indexes = new int[request.query.divideTimeRangeIntoNSegments + 1];
        for (int i = 0; i < indexes.length; i++) {
            indexes[i] = Math.abs(timeIndex.getClosestId(currentTime)); // handle negative "theoretical insertion" index
            currentTime += segmentDuration;
        }

        Map<String, LumberyardAnswer.Waveform> waveforms = Maps.newHashMap();
        start = System.currentTimeMillis();
        for (Map.Entry<String, MiruFilter> entry : request.query.lumberyardFilters.entrySet()) {
            LumberyardAnswer.Waveform waveform = null;
            if (!bitmaps.isEmpty(constrained)) {
                BM waveformFiltered = bitmaps.create();
                aggregateUtil.filter(bitmaps, context.getSchema(), context.getTermComposer(), context.getFieldIndexProvider(), entry.getValue(), solutionLog,
                    waveformFiltered, context.getActivityIndex().lastId(), -1);
                BM answer = bitmaps.create();
                bitmaps.and(answer, Arrays.asList(constrained, waveformFiltered));
                if (!bitmaps.isEmpty(answer)) {
                    waveform = lumberyard.lumberyarding(bitmaps, context, request.tenantId, answer, request.query.desiredNumberOfResultsPerWaveform, indexes);
                    if (solutionLog.isLogLevelEnabled(MiruSolutionLogLevel.DEBUG)) {
                        solutionLog.log(MiruSolutionLogLevel.DEBUG, "lumberyard answer: {} items.", bitmaps.cardinality(answer));
                        solutionLog.log(MiruSolutionLogLevel.DEBUG, "lumberyard name: {}, waveform: {}.", entry.getKey(), Arrays.toString(waveform.waveform));
                    }
                } else {
                    solutionLog.log(MiruSolutionLogLevel.DEBUG, "lumberyard empty answer.");
                }
            }
            if (waveform == null) {
                waveform = new LumberyardAnswer.Waveform(new long[request.query.divideTimeRangeIntoNSegments], Collections.<MiruActivity>emptyList());
            }
            waveforms.put(entry.getKey(), waveform);
        }
        solutionLog.log(MiruSolutionLogLevel.INFO, "lumberyard answered: {} millis.", System.currentTimeMillis() - start);
        solutionLog.log(MiruSolutionLogLevel.INFO, "lumberyard answered: {} iterations.", request.query.lumberyardFilters.size());

        LumberyardAnswer result = new LumberyardAnswer(waveforms, resultsExhausted);

        return new MiruPartitionResponse<>(result, solutionLog.asList());
    }

    @Override
    public MiruPartitionResponse<LumberyardAnswer> askRemote(RequestHelper requestHelper, MiruPartitionId partitionId, Optional<LumberyardReport> report)
        throws Exception {
        return new LumberyardRemotePartitionReader(requestHelper).scoreLumberyarding(partitionId, request, report);
    }

    @Override
    public Optional<LumberyardReport> createReport(Optional<LumberyardAnswer> answer) {
        Optional<LumberyardReport> report = Optional.absent();
        if (answer.isPresent()) {
            report = Optional.of(new LumberyardReport());
        }
        return report;
    }

    private boolean timeIndexIntersectsTimeRange(MiruTimeIndex timeIndex, MiruTimeRange timeRange) {
        return timeRange.smallestTimestamp <= timeIndex.getLargestTimestamp()
            && timeRange.largestTimestamp >= timeIndex.getSmallestTimestamp();
    }
}
