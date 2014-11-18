package com.jivesoftware.os.miru.analytics.plugins.analytics;

import com.google.common.base.Optional;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.analytics.plugins.analytics.AnalyticsAnswer.Waveform;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.MiruTimeIndex;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import java.util.Arrays;

/**
 *
 */
public class Analytics {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    public <BM> AnalyticsAnswer analyticing(MiruBitmaps<BM> bitmaps,
        MiruRequestContext<BM> requestContext,
        MiruRequest<AnalyticsQuery> request,
        Optional<AnalyticsReport> lastReport,
        BM answer,
        MiruSolutionLog solutionLog)
        throws Exception {

        log.debug("Get analyticing for answer={} query={}", answer, request);

        AnalyticsQuery query = request.query;
        long[] waveform = new long[query.divideTimeRangeIntoNSegments];

        MiruTimeRange timeRange = query.timeRange;
        long segmentDuration = (timeRange.largestTimestamp - timeRange.smallestTimestamp) / query.divideTimeRangeIntoNSegments;
        if (segmentDuration < 1) {
            throw new RuntimeException("Time range is insufficent to be divided into " + query.divideTimeRangeIntoNSegments + " segments");
        }
        
        MiruTimeIndex timeIndex = requestContext.getTimeIndex();
        long timeIndexSmallest = timeIndex.getSmallestTimestamp();
        long timeIndexLargest = timeIndex.getLargestTimestamp();
        
        long time = timeRange.smallestTimestamp;
        for (int i = 0; i < waveform.length; i++) {
            if (contains(timeIndexSmallest, timeIndexLargest, time) || contains(timeIndexSmallest, timeIndexLargest, time + segmentDuration)) {
                BM timeMask = bitmaps.buildTimeRangeMask(timeIndex, time, time + segmentDuration);
                BM count = bitmaps.create();
                bitmaps.and(count, Arrays.asList(answer, timeMask));
                waveform[i] = bitmaps.cardinality(count);
            }
            time += segmentDuration;
        }

//        MiruIntIterator iter = bitmaps.intIterator(answer);
//        while (iter.hasNext()) {
//            int index = iter.next();
//            long timestamp = requestContext.getTimeIndex().getTimestamp(index);
//            int ti = (int) ((waveform.length) * zeroToOne(request.query.timeRange.smallestTimestamp,
//                request.query.timeRange.largestTimestamp, timestamp));
//            if (ti > -1 && ti < waveform.length) {
//                waveform[ti]++;
//            }
//        }
        boolean resultsExhausted = request.query.timeRange.smallestTimestamp >= requestContext.getTimeIndex().getSmallestTimestamp();
        AnalyticsAnswer result = new AnalyticsAnswer(new Waveform(waveform), resultsExhausted);
        log.debug("result={}", result);
        return result;
    }

    boolean contains(long min, long max, long value) {
        if (value < min || value > max) {
            return false;
        } else {
            return true;
        }
    }

    public static double zeroToOne(long _min, long _max, long _long) {
        if (_max == _min) {
            if (_long == _min) {
                return 0;
            }
            if (_long > _max) {
                return Double.MAX_VALUE;
            }
            return -Double.MAX_VALUE;
        }
        return (double) (_long - _min) / (double) (_max - _min);
    }

}
