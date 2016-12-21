package com.jivesoftware.os.miru.metric.sampler;

import java.io.IOException;

import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import org.merlin.config.Config;
import org.merlin.config.defaults.BooleanDefault;
import org.merlin.config.defaults.IntDefault;

public class MiruMetricSamplerInitializer {

    public interface MiruMetricSamplerConfig extends Config {

        @IntDefault(60_000)
        int getSampleIntervalInMillis();

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
        TenantAwareHttpClient<String> client) throws IOException {

        if (config.getEnabled()) {
            return new HttpMiruMetricSampler(datacenter,
                cluster,
                host,
                service,
                instance,
                version,
                client,
                config.getSampleIntervalInMillis(),
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
