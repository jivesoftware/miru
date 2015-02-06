package com.jivesoftware.os.miru.analytics.plugins.metrics;

import com.jivesoftware.os.miru.analytics.plugins.metrics.MetricsAnswer.Waveform;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.List;

/**
 *
 */
public class Metrics {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    public <BM> Waveform metricing(MiruBitmaps<BM> bitmaps,
        List<BM> answers,
        int[] indexes,
        int numBits)
        throws Exception {

        log.debug("Get metricing for answers={}", answers);

        long[] waveform = new long[indexes.length];
        for (int i = 0; i < numBits; i++) {
            BM answer = answers.get(i);
            if (answer != null) {
                int multiplier = 1 << (numBits - 1 - i);
                long[] cardinalities = bitmaps.boundedCardinalities(answer, indexes);
                for (int j = 0; j < indexes.length; j++) {
                    waveform[j] += multiplier * cardinalities[j];
                }
            }
        }

        return new Waveform(waveform);
    }
}
