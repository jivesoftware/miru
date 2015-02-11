package com.jivesoftware.os.miru.metric.sampler;

import java.io.IOException;
import org.merlin.config.Config;
import org.merlin.config.defaults.BooleanDefault;
import org.merlin.config.defaults.IntDefault;
import org.merlin.config.defaults.StringDefault;

/**
 *
 */
public class MiruMetricSamplerInitializer {

    public static interface MiruMetricSamplerConfig extends Config {

        @StringDefault("undefined")
        String getMiruSeaAnomalyHostPorts();

        @IntDefault(60_000)
        int getSocketTimeoutInMillis();

        @IntDefault(5_000)
        int getSampleIntervalInMillis();

        @IntDefault(100)
        int getMaxBacklog();

        @BooleanDefault(true)
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
        final MiruMetricSamplerConfig config) throws IOException {

        if (config.getEnabled()) {

            String[] hostPorts = config.getMiruSeaAnomalyHostPorts().split("\\s*,\\s*");
            MiruMetricSampleSender[] sampleSenders = new MiruMetricSampleSender[hostPorts.length];

            for (int i = 0; i < sampleSenders.length; i++) {
                String[] parts = hostPorts[i].split(":");
                sampleSenders[i] = new HttpPoster(parts[0], Integer.parseInt(parts[1]), config.getSocketTimeoutInMillis());
            }

            return new HttpMiruMetricSampler(datacenter,
                cluster,
                host,
                service,
                instance,
                version,
                sampleSenders,
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
