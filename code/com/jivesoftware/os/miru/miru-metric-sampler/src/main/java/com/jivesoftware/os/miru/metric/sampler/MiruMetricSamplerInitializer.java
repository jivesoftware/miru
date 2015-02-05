package com.jivesoftware.os.miru.metric.sampler;

import java.io.IOException;
import org.merlin.config.Config;
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

        @IntDefault(1000)
        int getMaxBacklog();

    }

    public MiruMetricSampler initialize(String datacenter,
        String cluster,
        String host,
        String service,
        String instance,
        String version,
        MiruMetricSamplerConfig config) throws IOException {

        String[] hostPorts = config.getMiruSeaAnomalyHostPorts().split("\\s*,\\s*");
        MiruMetricSampleSender[] sampleSenders = new MiruMetricSampleSender[hostPorts.length];

        for (int i = 0; i < sampleSenders.length; i++) {
            String[] parts = hostPorts[i].split(":");
            sampleSenders[i] = new HttpPoster(parts[0], Integer.parseInt(parts[1]), config.getSocketTimeoutInMillis());
        }

        return new MiruMetricSampler(datacenter,
            cluster,
            host,
            service,
            instance,
            version,
            sampleSenders,
            config.getSampleIntervalInMillis(),
            config.getMaxBacklog());
    }

}
