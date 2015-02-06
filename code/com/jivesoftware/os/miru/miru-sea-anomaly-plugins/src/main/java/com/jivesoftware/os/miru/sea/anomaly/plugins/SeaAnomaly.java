package com.jivesoftware.os.miru.sea.anomaly.plugins;

import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.MiruProvider;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.sea.anomaly.plugins.SeaAnomalyAnswer.Waveform;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;

/**
 *
 */
public class SeaAnomaly {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final MiruProvider miruProvider;

    public SeaAnomaly(MiruProvider miruProvider) {
        this.miruProvider = miruProvider;
    }

    public <BM> Waveform anomalying(MiruBitmaps<BM> bitmaps,
        MiruRequestContext<BM> requestContext,
        MiruTenantId tenantId,
        BM answer,
        int[] indexes)
        throws Exception {

        log.debug("Get anomalying for answer={}", answer);

        return new Waveform(bitmaps.boundedCardinalities(answer, indexes));
    }
}
