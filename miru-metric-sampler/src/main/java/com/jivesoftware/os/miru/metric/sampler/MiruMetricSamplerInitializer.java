package com.jivesoftware.os.miru.metric.sampler;

import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.merlin.config.Config;
import org.merlin.config.defaults.BooleanDefault;
import org.merlin.config.defaults.IntDefault;
import org.merlin.config.defaults.StringDefault;

/**
 *
 */
public class MiruMetricSamplerInitializer {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    public static interface MiruMetricSamplerConfig extends Config {

        @StringDefault("undefined:-1")
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
            List<MiruMetricSampleSender> sampleSenders = new ArrayList<>();
            for (String hostPort : hostPorts) {
                String[] parts = hostPort.split(":");
                try {
                    int port = Integer.parseInt(parts[1]);
                    if (port > 0) {
                        sampleSenders.add(new HttpPoster(parts[0], port, config.getSocketTimeoutInMillis()));
                    }
                } catch (NumberFormatException | IOException x) {
                    LOG.error("Failed initializing MiruMetricSampleSender. input=" + hostPort, x);
                }
            }

            return new HttpMiruMetricSampler(datacenter,
                cluster,
                host,
                service,
                instance,
                version,
                sampleSenders.toArray(new MiruMetricSampleSender[sampleSenders.size()]),
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
