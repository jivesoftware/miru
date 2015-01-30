package com.jivesoftware.os.miru.lumberyard.plugins;

import com.jivesoftware.os.miru.lumberyard.plugins.LumberyardAnswer.Waveform;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;

/**
 *
 */
public class Lumberyard {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    public <BM> Waveform lumberyarding(MiruBitmaps<BM> bitmaps,
        BM answer,
        int[] indexes)
        throws Exception {

        log.debug("Get lumberyarding for answer={}", answer);
        return new Waveform(bitmaps.boundedCardinalities(answer, indexes));
    }
}
