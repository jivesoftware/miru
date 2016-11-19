package com.jivesoftware.os.miru.syslog.deployable;

import com.jivesoftware.os.miru.logappender.MiruLogAppenderInitializer.MiruLogAppenderConfig;
import com.jivesoftware.os.miru.logappender.RoutingBirdLogSenderProvider;
import com.jivesoftware.os.routing.bird.deployable.InstanceConfig;
import org.merlin.config.Config;
import org.merlin.config.defaults.BooleanDefault;
import org.merlin.config.defaults.IntDefault;

class MiruSyslogIntakeInitializer {

    interface MiruSyslogIntakeConfig extends Config {

        @BooleanDefault(false)
        boolean getEnabled();

        @IntDefault(2 * 1024 * 1024)
        int getMaxFrameLength();

        @BooleanDefault(false)
        boolean getIngressKeepAlive();

        @IntDefault(1024 * 1024)
        int getReceiveBufferSize();

    }

    MiruSyslogIntakeService initialize(InstanceConfig instanceConfig,
                                       MiruSyslogIntakeConfig miruSyslogIntakeConfig,
                                       MiruLogAppenderConfig miruLogAppenderConfig,
                                       RoutingBirdLogSenderProvider routingBirdLogSenderProvider) {
        return new MiruSyslogIntakeService(
                miruSyslogIntakeConfig.getEnabled(),
                instanceConfig.getMainPort(),
                miruSyslogIntakeConfig.getIngressKeepAlive(),
                miruSyslogIntakeConfig.getReceiveBufferSize(),
                miruSyslogIntakeConfig.getMaxFrameLength(),
                miruLogAppenderConfig.getQueueMaxDepth(),
                miruLogAppenderConfig.getBatchSize(),
                miruLogAppenderConfig.getQueueIsBlocking(),
                miruLogAppenderConfig.getIfSuccessPauseMillis(),
                miruLogAppenderConfig.getIfEmptyPauseMillis(),
                miruLogAppenderConfig.getIfErrorPauseMillis(),
                miruLogAppenderConfig.getCycleReceiverAfterAppendCount(),
                miruLogAppenderConfig.getNonBlockingDrainThreshold(),
                miruLogAppenderConfig.getNonBlockingDrainCount(),
                routingBirdLogSenderProvider);
    }

}
