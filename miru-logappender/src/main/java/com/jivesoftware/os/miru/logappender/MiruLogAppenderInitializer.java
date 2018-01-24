package com.jivesoftware.os.miru.logappender;

import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import org.merlin.config.Config;
import org.merlin.config.defaults.BooleanDefault;
import org.merlin.config.defaults.IntDefault;
import org.merlin.config.defaults.LongDefault;

public class MiruLogAppenderInitializer {

    public interface MiruLogAppenderConfig extends Config {
        @IntDefault(100_000)
        int getQueueMaxDepth();

        @IntDefault(10_000)
        int getBatchSize();

        @BooleanDefault(false)
        boolean getQueueIsBlocking();

        @LongDefault(1_000)
        long getIfSuccessPauseMillis();

        @LongDefault(1_000)
        long getIfEmptyPauseMillis();

        @LongDefault(5_000)
        long getIfErrorPauseMillis();

        @IntDefault(1_000)
        int getNonBlockingDrainThreshold();

        @IntDefault(10_000)
        int getNonBlockingDrainCount();

        @BooleanDefault(false)
        boolean getEnabled();
    }

    public MiruLogAppender initialize(String datacenter,
        String cluster,
        String host,
        String service,
        String instance,
        String version,
        MiruLogAppenderConfig config,
        TenantAwareHttpClient<String> client) {
        if (config.getEnabled()) {
            return new HttpMiruLogAppender(datacenter,
                cluster,
                host,
                service,
                instance,
                version,
                client,
                config.getQueueMaxDepth(),
                config.getBatchSize(),
                config.getQueueIsBlocking(),
                config.getIfSuccessPauseMillis(),
                config.getIfEmptyPauseMillis(),
                config.getIfErrorPauseMillis(),
                config.getNonBlockingDrainThreshold(),
                config.getNonBlockingDrainCount());
        } else {
            return () -> {};
        }
    }

}
