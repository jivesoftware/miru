package com.jivesoftware.os.miru.analytics.plugins.metrics;

import com.jivesoftware.os.miru.analytics.plugins.metrics.MetricsAnswer.Waveform;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;

/**
 *
 */
public class Metrics {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    public <BM> Waveform metricing(MiruBitmaps<BM> bitmaps,
        BM answer,
        int[] indexes)
        throws Exception {

        log.debug("Get metricing for answer={}", answer);
        return new Waveform(bitmaps.boundedCardinalities(answer, indexes));
    }
}
