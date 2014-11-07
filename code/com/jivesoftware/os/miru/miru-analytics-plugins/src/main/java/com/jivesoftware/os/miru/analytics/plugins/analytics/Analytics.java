package com.jivesoftware.os.miru.analytics.plugins.analytics;

import com.google.common.base.Optional;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.analytics.plugins.analytics.AnalyticsAnswer.Waveform;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.MiruIntIterator;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;

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

        long[] waveform = new long[request.query.divideTimeRangeIntoNSegments];
        MiruIntIterator iter = bitmaps.intIterator(answer);
        while (iter.hasNext()) {
            int index = iter.next();
            long timestamp = requestContext.getTimeIndex().getTimestamp(index);
            int ti = (int) ((waveform.length) * zeroToOne(request.query.timeRange.smallestTimestamp,
                request.query.timeRange.largestTimestamp, timestamp));
            if (ti > -1 && ti < waveform.length) {
                waveform[ti]++;
            }
        }

        boolean resultsExhausted = request.query.timeRange.smallestTimestamp >= requestContext.getTimeIndex().getSmallestTimestamp();
        AnalyticsAnswer result = new AnalyticsAnswer(new Waveform(waveform), resultsExhausted);
        log.debug("result={}", result);
        return result;
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
