package com.jivesoftware.os.miru.logappender;

import java.io.IOException;
import org.merlin.config.Config;
import org.merlin.config.defaults.BooleanDefault;
import org.merlin.config.defaults.IntDefault;
import org.merlin.config.defaults.LongDefault;
import org.merlin.config.defaults.StringDefault;

/**
 *
 */
public class MiruLogAppenderInitializer {

    public static interface MiruLogAppenderConfig extends Config {

        @StringDefault("undefinedHost:-1")
        String getMiruStumptownHostPorts();

        @IntDefault(60_000)
        int getSocketTimeoutInMillis();

        @IntDefault(-1)
        int getMaxConnections();

        @IntDefault(1)
        int getMaxConnectionsPerHost();

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

        @LongDefault(1_000)
        long getCycleReceiverAfterAppendCount();

        @IntDefault(1_000)
        int getNonBlockingDrainThreshold();

        @IntDefault(10_000)
        int getNonBlockingDrainCount();
    }

    public MiruLogAppender initialize(String datacenter,
        String cluster,
        String host,
        String service,
        String instance,
        String version,
        MiruLogAppenderConfig config) throws IOException {

        String[] hostPorts = config.getMiruStumptownHostPorts().split("\\s*,\\s*");
        MiruLogSender[] logSenders = new MiruLogSender[hostPorts.length];

        for (int i = 0; i < logSenders.length; i++) {
            String[] parts = hostPorts[i].split(":");
            logSenders[i] = new HttpPoster(parts[0], Integer.parseInt(parts[1]), config.getSocketTimeoutInMillis());
        }

        return new MiruLogAppender(datacenter,
            cluster,
            host,
            service,
            instance,
            version,
            logSenders,
            config.getQueueMaxDepth(),
            config.getBatchSize(),
            config.getQueueIsBlocking(),
            config.getIfSuccessPauseMillis(),
            config.getIfEmptyPauseMillis(),
            config.getIfErrorPauseMillis(),
            config.getCycleReceiverAfterAppendCount(),
            config.getNonBlockingDrainThreshold(),
            config.getNonBlockingDrainCount());
    }

}
