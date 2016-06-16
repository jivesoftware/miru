package com.jivesoftware.os.miru.metric.sampler;

import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.IOException;
import org.merlin.config.Config;
import org.merlin.config.defaults.BooleanDefault;
import org.merlin.config.defaults.IntDefault;

/**
 *
 */
public class MiruMetricSamplerInitializer {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    public static interface MiruMetricSamplerConfig extends Config {

        @IntDefault(60_000)
        int getSocketTimeoutInMillis();

        @IntDefault(60_000)
        int getSampleIntervalInMillis();

        @IntDefault(100)
        int getMaxBacklog();

        @BooleanDefault(false)
        boolean getEnabled();

        @BooleanDefault(true)
        boolean getEnableJVMMetrics();

        @BooleanDefault(false)
        boolean getEnableTenantMetrics();

    }

    public MiruMetricSampler initialize(String datacenter,
        String cluster,
        String host,
        String service,
        String instance,
        String version,
        MiruMetricSamplerConfig config,
        MiruMetricSampleSenderProvider senderProvider) throws IOException {

        if (config.getEnabled()) {

            return new HttpMiruMetricSampler(datacenter,
                cluster,
                host,
                service,
                instance,
                version,
                senderProvider,
                config.getSampleIntervalInMillis(),
                config.getMaxBacklog(),
                config.getEnableTenantMetrics(),
                config.getEnableJVMMetrics());
        } else {
            return new MiruMetricSampler() {

                @Override
                public void start() {
                }

                @Override
                public void stop() {
                }

                @Override
                public boolean isStarted() {
                    return false;
                }
            };
        }
    }

}
