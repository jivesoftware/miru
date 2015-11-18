package com.jivesoftware.os.miru.analytics.plugins.analytics;

import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmapsDebug;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.MiruTimeIndex;
import com.jivesoftware.os.miru.plugin.solution.MiruAggregateUtil;
import com.jivesoftware.os.miru.plugin.solution.MiruRequestHandle;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.plugin.solution.Waveform;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 *
 */
public class Analytics {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final MiruAggregateUtil aggregateUtil = new MiruAggregateUtil();
    private final MiruBitmapsDebug bitmapsDebug = new MiruBitmapsDebug();

    public interface Analysis<T> {
        boolean consume(ToAnalyze<T> toAnalyze) throws Exception;
    }

    public interface ToAnalyze<T> {
        boolean analyze(T term, long version, MiruFilter filter) throws Exception;
    }

    public interface Analyzed<T> {
        boolean analyzed(T term, long version, Waveform waveform) throws Exception;
    }

    public <BM, T> boolean analyze(MiruSolutionLog solutionLog,
        MiruRequestHandle<BM, ?> handle,
        MiruRequestContext<BM, ?> context,
        MiruAuthzExpression authzExpression,
        MiruTimeRange timeRange,
        MiruFilter constraintsFilter,
        int divideTimeRangeIntoNSegments,
        Analysis<T> analysis,
        Analyzed<T> analyzed) throws Exception {

        MiruBitmaps<BM> bitmaps = handle.getBitmaps();
        MiruPartitionCoord coord = handle.getCoord();
        MiruTimeIndex timeIndex = context.getTimeIndex();

        // Short-circuit if this is not a properly bounded query
        if (timeRange.largestTimestamp == Long.MAX_VALUE || timeRange.smallestTimestamp == 0) {
            solutionLog.log(MiruSolutionLogLevel.WARN, "Improperly bounded query: {}", timeRange);
            analysis.consume((termId, version, filter) -> analyzed.analyzed(termId, version, new Waveform(new long[divideTimeRangeIntoNSegments])));
            return true;
        }

        // Short-circuit if the time range doesn't live here
        boolean resultsExhausted = timeRange.smallestTimestamp > timeIndex.getLargestTimestamp();
        if (!timeIndex.intersects(timeRange)) {
            solutionLog.log(MiruSolutionLogLevel.WARN, "No time index intersection. Partition {}: {} doesn't intersect with {}",
                coord.partitionId, timeIndex, timeRange);
            analysis.consume((termId, version, filter) -> analyzed.analyzed(termId, version, new Waveform(new long[divideTimeRangeIntoNSegments])));
            return resultsExhausted;
        }

        // Start building up list of bitmap operations to run
        List<BM> ands = new ArrayList<>();

        long start = System.currentTimeMillis();
        ands.add(bitmaps.buildTimeRangeMask(timeIndex, timeRange.smallestTimestamp, timeRange.largestTimestamp));
        solutionLog.log(MiruSolutionLogLevel.INFO, "analytics timeRangeMask: {} millis.", System.currentTimeMillis() - start);

        // 1) Execute the combined filter above on the given stream, add the bitmap
        if (MiruFilter.NO_FILTER.equals(constraintsFilter)) {
            solutionLog.log(MiruSolutionLogLevel.INFO, "analytics filter: no constraints.");
        } else {
            BM filtered = bitmaps.create();
            start = System.currentTimeMillis();
            aggregateUtil.filter(bitmaps, context.getSchema(), context.getTermComposer(), context.getFieldIndexProvider(), constraintsFilter,
                solutionLog, filtered, null, context.getActivityIndex().lastId(), -1);
            solutionLog.log(MiruSolutionLogLevel.INFO, "analytics filter: {} millis.", System.currentTimeMillis() - start);
            ands.add(filtered);
        }

        // 2) Add in the authz check if we have it
        if (!MiruAuthzExpression.NOT_PROVIDED.equals(authzExpression)) {
            ands.add(context.getAuthzIndex().getCompositeAuthz(authzExpression));
        }

        // 3) Mask out anything that hasn't made it into the activityIndex yet, or that has been removed from the index
        start = System.currentTimeMillis();
        ands.add(bitmaps.buildIndexMask(context.getActivityIndex().lastId(), context.getRemovalIndex().getIndex()));
        solutionLog.log(MiruSolutionLogLevel.INFO, "analytics indexMask: {} millis.", System.currentTimeMillis() - start);

        // AND it all together to get the final constraints
        BM constrained = bitmaps.create();
        bitmapsDebug.debug(solutionLog, bitmaps, "ands", ands);
        start = System.currentTimeMillis();
        bitmaps.and(constrained, ands);
        solutionLog.log(MiruSolutionLogLevel.INFO, "analytics constrained: {} millis.", System.currentTimeMillis() - start);

        if (solutionLog.isLogLevelEnabled(MiruSolutionLogLevel.INFO)) {
            solutionLog.log(MiruSolutionLogLevel.INFO, "analytics constrained {} items.", bitmaps.cardinality(constrained));
        }

        long currentTime = timeRange.smallestTimestamp;
        long segmentDuration = (timeRange.largestTimestamp - timeRange.smallestTimestamp) / divideTimeRangeIntoNSegments;
        if (segmentDuration < 1) {
            throw new RuntimeException("Time range is insufficient to be divided into " + divideTimeRangeIntoNSegments + " segments");
        }

        start = System.currentTimeMillis();
        int[] indexes = new int[divideTimeRangeIntoNSegments + 1];
        for (int i = 0; i < indexes.length; i++) {
            indexes[i] = Math.abs(timeIndex.getClosestId(currentTime)); // handle negative "theoretical insertion" index
            currentTime += segmentDuration;
        }
        solutionLog.log(MiruSolutionLogLevel.INFO, "analytics bucket boundaries: {} millis.", System.currentTimeMillis() - start);

        start = System.currentTimeMillis();
        int[] count = new int[1];
        analysis.consume((term, version, filter) -> {
            Waveform waveform = null;
            if (!bitmaps.isEmpty(constrained)) {
                BM waveformFiltered = bitmaps.create();
                aggregateUtil.filter(bitmaps, context.getSchema(), context.getTermComposer(), context.getFieldIndexProvider(), filter, solutionLog,
                    waveformFiltered, null, context.getActivityIndex().lastId(), -1);
                BM answer;
                if (bitmaps.supportsInPlace()) {
                    answer = waveformFiltered;
                    bitmaps.inPlaceAnd(waveformFiltered, constrained);
                } else {
                    answer = bitmaps.create();
                    bitmaps.and(answer, Arrays.asList(constrained, waveformFiltered));
                }
                if (!bitmaps.isEmpty(answer)) {
                    waveform = analytics(bitmaps, answer, indexes);
                    if (solutionLog.isLogLevelEnabled(MiruSolutionLogLevel.DEBUG)) {
                        solutionLog.log(MiruSolutionLogLevel.DEBUG, "analytics answer: {} items.", bitmaps.cardinality(answer));
                        solutionLog.log(MiruSolutionLogLevel.DEBUG, "analytics name: {}, waveform: {}.", term, waveform);
                    }
                } else {
                    solutionLog.log(MiruSolutionLogLevel.DEBUG, "analytics empty answer.");
                }
            }
            if (waveform == null) {
                waveform = new Waveform(new long[divideTimeRangeIntoNSegments]);
            }
            count[0]++;
            return analyzed.analyzed(term, version, waveform);
        });
        solutionLog.log(MiruSolutionLogLevel.INFO, "analytics answered: {} millis.", System.currentTimeMillis() - start);
        solutionLog.log(MiruSolutionLogLevel.INFO, "analytics answered: {} iterations.", count[0]);

        return resultsExhausted;
    }

    private <BM> Waveform analytics(MiruBitmaps<BM> bitmaps,
        BM answer,
        int[] indexes)
        throws Exception {

        log.debug("Get analytics for answer={}", answer);
        return new Waveform(bitmaps.boundedCardinalities(answer, indexes));
    }

}
