package com.jivesoftware.os.miru.stumptown.deployable;

import com.jivesoftware.os.filer.queue.guaranteed.delivery.DeliveryCallback;
import com.jivesoftware.os.filer.queue.guaranteed.delivery.FileQueueBackGuaranteedDeliveryFactory;
import com.jivesoftware.os.filer.queue.guaranteed.delivery.FileQueueBackGuaranteedDeliveryServiceConfig;
import com.jivesoftware.os.filer.queue.guaranteed.delivery.GuaranteedDeliveryService;
import com.jivesoftware.os.filer.queue.guaranteed.delivery.GuaranteedDeliveryServiceStatus;
import com.jivesoftware.os.filer.queue.processor.PhasedQueueProcessorConfig;
import com.jivesoftware.os.mlogger.core.Counter;
import com.jivesoftware.os.mlogger.core.ValueType;
import com.jivesoftware.os.routing.bird.health.api.HealthCheckUtil;
import com.jivesoftware.os.routing.bird.health.api.HealthChecker;
import com.jivesoftware.os.routing.bird.health.api.HealthFactory;
import com.jivesoftware.os.routing.bird.health.api.MinMaxHealthChecker;
import com.jivesoftware.os.routing.bird.health.api.ScheduledHealthCheck;
import com.jivesoftware.os.routing.bird.health.api.ScheduledMinMaxHealthCheckConfig;
import com.jivesoftware.os.routing.bird.health.checkers.DiskFreeHealthChecker;
import java.io.File;
import java.io.IOException;
import org.merlin.config.defaults.LongDefault;
import org.merlin.config.defaults.StringDefault;

class IngressGuaranteedDeliveryQueueProvider {

    interface IngressDiskCheck extends ScheduledMinMaxHealthCheckConfig {

        @StringDefault("ingress>disk")
        @Override
        String getName();

        @LongDefault(80)
        @Override
        Long getMax();

    }

    interface IngressPendingCheck extends ScheduledMinMaxHealthCheckConfig {

        @StringDefault("ingress>pending")
        @Override
        String getName();

        @LongDefault(100000)
        @Override
        Long getMax();

    }

    private final GuaranteedDeliveryService[] guaranteedDeliveryServices;

    public IngressGuaranteedDeliveryQueueProvider(final String pathToQueues,
        int numberOfPartitions,
        int numberOfSendThreadsPerPartition,
        DeliveryCallback deliveryCallback) throws IOException {

        HealthFactory.scheduleHealthChecker(IngressDiskCheck.class,
            config -> (HealthChecker) new DiskFreeHealthChecker(config, new File[] { new File(pathToQueues) }));

        this.guaranteedDeliveryServices = new GuaranteedDeliveryService[numberOfPartitions];
        for (int i = 0; i < guaranteedDeliveryServices.length; i++) {
            guaranteedDeliveryServices[i] = FileQueueBackGuaranteedDeliveryFactory.createService(
                FileQueueBackGuaranteedDeliveryServiceConfig
                    .newBuilder(pathToQueues, "stumptownQueue-" + i,
                        PhasedQueueProcessorConfig
                            .newBuilder("stumptownQueue-" + i)
                            .setIdealMinimumBatchSize(1000)
                            .setIdealMinimumBatchSizeMaxWaitMillis(1000)
                            .setMaxBatchSize(10000)
                            .build())
                    .setNumberOfConsumers(numberOfSendThreadsPerPartition)
                    .build(),
                deliveryCallback);
        }

        HealthFactory.scheduleHealthChecker(IngressPendingCheck.class,
            config -> (HealthChecker) new PendingHealthChecker(guaranteedDeliveryServices, config));
    }

    private static class PendingHealthChecker extends MinMaxHealthChecker implements ScheduledHealthCheck {

        private final GuaranteedDeliveryService[] guaranteedDeliveryServices;
        private final ScheduledMinMaxHealthCheckConfig config;

        PendingHealthChecker(GuaranteedDeliveryService[] guaranteedDeliveryServices, ScheduledMinMaxHealthCheckConfig config) {
            super(config);
            this.guaranteedDeliveryServices = guaranteedDeliveryServices;
            this.config = config;
        }

        @Override
        public long getCheckIntervalInMillis() {
            return config.getCheckIntervalInMillis();
        }

        @Override
        public void run() {
            try {
                StringBuilder sb = new StringBuilder();
                long maxUndelivered = 0;
                for (int i = 0; i < guaranteedDeliveryServices.length; i++) {
                    GuaranteedDeliveryService guaranteedDeliveryService = guaranteedDeliveryServices[i];
                    GuaranteedDeliveryServiceStatus status = guaranteedDeliveryService.getStatus();

                    double percentageUsed = HealthCheckUtil.zeroToOne(0, config.getMax(), status.undelivered());
                    sb.append("queue:").append(i).append(" at ").append(100 * percentageUsed).append("% used. ");
                    if (maxUndelivered < status.undelivered()) {
                        maxUndelivered = status.undelivered();
                    }
                }
                Counter counter = new Counter(ValueType.RATE);
                counter.set(maxUndelivered);
                check(counter, sb.toString(), "unknown");
            } catch (Exception x) {
                // TODO what?
            }
        }

    }

    GuaranteedDeliveryService getGuaranteedDeliveryServices(int hash) {
        return guaranteedDeliveryServices[Math.abs(hash % guaranteedDeliveryServices.length)];
    }

}
