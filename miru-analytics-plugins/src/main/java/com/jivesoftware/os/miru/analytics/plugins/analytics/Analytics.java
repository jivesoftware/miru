package com.jivesoftware.os.miru.analytics.plugins.analytics;

import com.jivesoftware.os.miru.plugin.solution.Waveform;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;

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
