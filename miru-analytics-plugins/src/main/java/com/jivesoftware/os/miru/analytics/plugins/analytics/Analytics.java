package com.jivesoftware.os.miru.analytics.plugins.analytics;

import com.jivesoftware.os.filer.io.api.StackBuffer;
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

    public interface Analysis<T, BM> {

        boolean consume(ToAnalyze<T, BM> toAnalyze) throws Exception;
    }

    public interface ToAnalyze<T, BM> {

        boolean analyze(T term, BM bitmap) throws Exception;
    }

    public interface Analyzed<T> {

        boolean analyzed(int index, T term, long[] waveformBuffer) throws Exception;
    }

    public static class AnalyticsScoreable {
        public final MiruTimeRange timeRange;
        public final int divideTimeRangeIntoNSegments;

        public AnalyticsScoreable(MiruTimeRange timeRange, int divideTimeRangeIntoNSegments) {
            this.timeRange = timeRange;
            this.divideTimeRangeIntoNSegments = divideTimeRangeIntoNSegments;
        }
    }

    public <BM extends IBM, IBM, T> boolean analyze(String name,
        MiruSolutionLog solutionLog,
        MiruRequestHandle<BM, IBM, ?> handle,
        MiruRequestContext<BM, IBM, ?> context,
        MiruAuthzExpression authzExpression,
        MiruTimeRange timeRange,
        MiruFilter constraintsFilter,
        AnalyticsScoreable[] scoreables,
        StackBuffer stackBuffer,
        Analysis<T, BM> analysis,
        Analyzed<T> analyzed) throws Exception {

        MiruBitmaps<BM, IBM> bitmaps = handle.getBitmaps();
        MiruPartitionCoord coord = handle.getCoord();
        MiruTimeIndex timeIndex = context.getTimeIndex();

        // Short-circuit if this is not a properly bounded query
        if (timeRange.largestTimestamp == Long.MAX_VALUE || timeRange.smallestTimestamp == 0) {
            solutionLog.log(MiruSolutionLogLevel.WARN, "Improperly bounded query: {}", timeRange);
            analysis.consume((termId, filter) -> {
                for (int i = 0; i < scoreables.length; i++) {
                    if (!analyzed.analyzed(i, termId, null)) {
                        return false;
                    }
                }
                return true;
            });
            return true;
        }

        // Short-circuit if the time range doesn't live here
        boolean resultsExhausted = timeRange.smallestTimestamp > timeIndex.getLargestTimestamp();
        if (!timeIndex.intersects(timeRange)) {
            solutionLog.log(MiruSolutionLogLevel.WARN, "No time index intersection. Partition {}: {} doesn't intersect with {}",
                coord.partitionId, timeIndex, timeRange);
            analysis.consume((termId, filter) -> {
                for (int i = 0; i < scoreables.length; i++) {
                    if (!analyzed.analyzed(i, termId, null)) {
                        return false;
                    }
                }
                return true;
            });
            return resultsExhausted;
        }

        // Start building up list of bitmap operations to run
        List<IBM> ands = new ArrayList<>();

        int lastId = context.getActivityIndex().lastId(stackBuffer);

        long start = System.currentTimeMillis();
        ands.add(bitmaps.buildTimeRangeMask(timeIndex, timeRange.smallestTimestamp, timeRange.largestTimestamp, stackBuffer));
        solutionLog.log(MiruSolutionLogLevel.INFO, "analytics timeRangeMask: {} millis.", System.currentTimeMillis() - start);

        // 1) Execute the combined filter above on the given stream, add the bitmap
        if (MiruFilter.NO_FILTER.equals(constraintsFilter)) {
            solutionLog.log(MiruSolutionLogLevel.INFO, "analytics filter: no constraints.");
        } else {
            start = System.currentTimeMillis();
            BM filtered = aggregateUtil.filter(name, bitmaps, context, constraintsFilter, solutionLog, null, lastId, -1, -1, stackBuffer);
            solutionLog.log(MiruSolutionLogLevel.INFO, "analytics filter: {} millis.", System.currentTimeMillis() - start);
            ands.add(filtered);
        }

        // 2) Add in the authz check if we have it
        if (!MiruAuthzExpression.NOT_PROVIDED.equals(authzExpression)) {
            ands.add(context.getAuthzIndex().getCompositeAuthz(authzExpression, stackBuffer));
        }

        // 3) Mask out anything that hasn't made it into the activityIndex yet, or that has been removed from the index
        start = System.currentTimeMillis();
        ands.add(bitmaps.buildIndexMask(lastId, context.getRemovalIndex(), null, stackBuffer));
        solutionLog.log(MiruSolutionLogLevel.INFO, "analytics indexMask: {} millis.", System.currentTimeMillis() - start);

        // AND it all together to get the final constraints
        bitmapsDebug.debug(solutionLog, bitmaps, "ands", ands);
        start = System.currentTimeMillis();
        BM constrained = bitmaps.and(ands);
        solutionLog.log(MiruSolutionLogLevel.INFO, "analytics constrained: {} millis.", System.currentTimeMillis() - start);

        if (solutionLog.isLogLevelEnabled(MiruSolutionLogLevel.INFO)) {
            solutionLog.log(MiruSolutionLogLevel.INFO, "analytics constrained {} items.", bitmaps.cardinality(constrained));
        }

        start = System.currentTimeMillis();
        int[][] indexes = new int[scoreables.length][];
        long[][] rawWaveformBuffer = new long[scoreables.length][];
        for (int i = 0; i < scoreables.length; i++) {
            AnalyticsScoreable scoreable = scoreables[i];
            long currentTime = scoreable.timeRange.smallestTimestamp;
            long segmentDuration = (scoreable.timeRange.largestTimestamp - scoreable.timeRange.smallestTimestamp) / scoreable.divideTimeRangeIntoNSegments;
            if (segmentDuration < 1) {
                throw new RuntimeException("Time range is insufficient to be divided into " + scoreable.divideTimeRangeIntoNSegments + " segments");
            }

            indexes[i] = new int[scoreable.divideTimeRangeIntoNSegments + 1];
            rawWaveformBuffer[i] = new long[scoreable.divideTimeRangeIntoNSegments];
            for (int j = 0; j < indexes[i].length; j++) {
                int closestId = timeIndex.getClosestId(currentTime, stackBuffer);
                if (closestId < 0) {
                    closestId = -(closestId + 1); // handle negative "theoretical insertion" index
                }
                indexes[i][j] = closestId;
                currentTime += segmentDuration;
            }
        }
        solutionLog.log(MiruSolutionLogLevel.INFO, "analytics bucket boundaries: {} millis.", System.currentTimeMillis() - start);

        start = System.currentTimeMillis();
        int[] count = new int[1];

        analysis.consume((term, waveformFiltered) -> {
            boolean found = false;
            if (!bitmaps.isEmpty(constrained)) {
                BM answer = waveformFiltered;
                bitmaps.inPlaceAnd(waveformFiltered, constrained);
                if (!bitmaps.isEmpty(answer)) {
                    found = true;
                    for (int i = 0; i < rawWaveformBuffer.length; i++) {
                        Arrays.fill(rawWaveformBuffer[i], 0);
                    }
                    bitmaps.boundedCardinalities(answer, indexes, rawWaveformBuffer);

                    if (solutionLog.isLogLevelEnabled(MiruSolutionLogLevel.DEBUG)) {
                        solutionLog.log(MiruSolutionLogLevel.DEBUG, "analytics answer: {} items.", bitmaps.cardinality(answer));
                        solutionLog.log(MiruSolutionLogLevel.DEBUG, "analytics name: {}, waveform: {}.", term, Arrays.toString(rawWaveformBuffer));
                    }
                } else {
                    solutionLog.log(MiruSolutionLogLevel.DEBUG, "analytics empty answer.");
                }
            }
            count[0]++;
            for (int i = 0; i < scoreables.length; i++) {
                if (!analyzed.analyzed(i, term, found ? rawWaveformBuffer[i] : null)) {
                    return false;
                }
            }
            return true;
        });
        solutionLog.log(MiruSolutionLogLevel.INFO, "analytics answered: {} millis.", System.currentTimeMillis() - start);
        solutionLog.log(MiruSolutionLogLevel.INFO, "analytics answered: {} iterations.", count[0]);

        return resultsExhausted;
    }

}
