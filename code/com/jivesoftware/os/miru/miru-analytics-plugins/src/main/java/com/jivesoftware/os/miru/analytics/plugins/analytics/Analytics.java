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
import org.apache.commons.lang.math.LongRange;

/**
 *
 */
public class Analytics {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    public <BM> Waveform analyticing(MiruBitmaps<BM> bitmaps,
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
            throw new RuntimeException("Time range is insufficient to be divided into " + query.divideTimeRangeIntoNSegments + " segments");
        }

        MiruTimeIndex timeIndex = requestContext.getTimeIndex();
        LongRange indexRange = new LongRange(timeIndex.getSmallestTimestamp(), timeIndex.getLargestTimestamp());

        long time = timeRange.smallestTimestamp;
        for (int i = 0; i < waveform.length; i++) {
            if (indexRange.overlapsRange(new LongRange(time, time + segmentDuration))) {
                BM timeMask = bitmaps.buildTimeRangeMask(timeIndex, time, time + segmentDuration);
                BM count = bitmaps.create();
                bitmaps.and(count, Arrays.asList(answer, timeMask));
                waveform[i] = bitmaps.cardinality(count);
            }
            time += segmentDuration;
        }

        return new Waveform(waveform);
    }
}
