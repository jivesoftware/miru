package com.jivesoftware.os.miru.kinesis.logappender;

import org.merlin.config.Config;
import org.merlin.config.defaults.BooleanDefault;
import org.merlin.config.defaults.IntDefault;
import org.merlin.config.defaults.LongDefault;
import org.merlin.config.defaults.StringDefault;

public class KinesisLogAppenderInitializer {

    public interface KinesisLogAppenderConfig extends Config {
        @IntDefault(100_000)
        int getQueueMaxDepth();

        @IntDefault(500)
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

        @StringDefault("")
        String getAwsRegion();

        @StringDefault("")
        String getAwsAccessKeyId();

        @StringDefault("")
        String getAwsSecretAccessKey();

        @StringDefault("")
        String getAwsStreamName();
    }

    public KinesisLogAppender initialize(KinesisLogAppenderConfig config) {
        if (config.getEnabled()) {
            return new HttpKinesisLogAppender(config.getQueueMaxDepth(),
                config.getBatchSize(),
                config.getQueueIsBlocking(),
                config.getIfSuccessPauseMillis(),
                config.getIfEmptyPauseMillis(),
                config.getIfErrorPauseMillis(),
                config.getNonBlockingDrainThreshold(),
                config.getNonBlockingDrainCount(),
                config.getAwsRegion(),
                config.getAwsAccessKeyId(),
                config.getAwsSecretAccessKey(),
                config.getAwsStreamName());
        } else {
            return () -> {};
        }
    }

}
