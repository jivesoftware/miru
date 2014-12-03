package com.jivesoftware.os.miru.analytics.plugins.analytics;

import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.analytics.plugins.analytics.AnalyticsAnswer.Waveform;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;

/**
 *
 */
public class Analytics {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    public <BM> Waveform analyticing(MiruBitmaps<BM> bitmaps,
        BM answer,
        int[] indexes)
        throws Exception {

        log.debug("Get analyticing for answer={}", answer);
        return new Waveform(bitmaps.boundedCardinalities(answer, indexes));
    }
}
