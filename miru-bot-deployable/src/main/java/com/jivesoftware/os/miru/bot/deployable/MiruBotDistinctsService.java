package com.jivesoftware.os.miru.bot.deployable;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.miru.bot.deployable.MiruBotDistinctsInitializer.MiruBotDistinctsConfig;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

class MiruBotDistinctsService implements MiruBotHealthPercent {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruBotDistinctsConfig miruBotDistinctsConfig;
    private final String miruIngressEndpoint;
    private final OrderIdProvider orderIdProvider;
    private final MiruBotSchemaService miruBotSchemaService;
    private final TenantAwareHttpClient<String> miruClientReader;
    private final TenantAwareHttpClient<String> miruClientWriter;

    private MiruBotDistinctsWorker miruBotDistinctsWorker;

    private final ScheduledExecutorService processor =
            Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder().setNameFormat("mirubot-distincts-%d").build());

    MiruBotDistinctsService(String miruIngressEndpoint,
                            MiruBotDistinctsConfig miruBotDistinctsConfig,
                            OrderIdProvider orderIdProvider,
                            MiruBotSchemaService miruBotSchemaService,
                            TenantAwareHttpClient<String> miruClientReader,
                            TenantAwareHttpClient<String> miruClientWriter) {
        this.miruIngressEndpoint = miruIngressEndpoint;
        this.miruBotDistinctsConfig = miruBotDistinctsConfig;
        this.orderIdProvider = orderIdProvider;
        this.miruBotSchemaService = miruBotSchemaService;
        this.miruClientReader = miruClientReader;
        this.miruClientWriter = miruClientWriter;
    }

    void start() {
        if (!miruBotDistinctsConfig.getEnabled()) {
            LOG.warn("Not starting distincts service; not enabled.");
            return;
        }

        LOG.info("Enabled: {}", miruBotDistinctsConfig.getEnabled());

        miruBotDistinctsWorker = createWithConfig(miruBotDistinctsConfig);
        processor.submit(miruBotDistinctsWorker);
    }

    public void stop() throws InterruptedException {
        miruBotDistinctsWorker.setRunning(false);
        Thread.sleep(miruBotDistinctsConfig.getWriteReadPauseMs() +
                miruBotDistinctsConfig.getFailureRetryWaitMs());
        processor.shutdownNow();
    }

    MiruBotDistinctsWorker createWithConfig(MiruBotDistinctsConfig miruBotDistinctsConfig) {
        LOG.info("Read time range: {}", miruBotDistinctsConfig.getReadTimeRange());
        LOG.info("Write hesitation factor: {}", miruBotDistinctsConfig.getWriteHesitationFactor());
        LOG.info("Value size factor: {}", miruBotDistinctsConfig.getValueSizeFactor());
        LOG.info("Failure retry wait: {}", miruBotDistinctsConfig.getFailureRetryWaitMs());
        LOG.info("Birth rate factor: {}", miruBotDistinctsConfig.getBirthRateFactor());
        LOG.info("Read frequency: {}", miruBotDistinctsConfig.getReadFrequency());
        LOG.info("Batch write count factor: {}", miruBotDistinctsConfig.getBatchWriteCountFactor());
        LOG.info("Batch write frequency: {}", miruBotDistinctsConfig.getBatchWriteFrequency());
        LOG.info("Number of fields: {}", miruBotDistinctsConfig.getNumberOfFields());
        LOG.info("Bot bucket seed: {}", miruBotDistinctsConfig.getBotBucketSeed());
        LOG.info("Write read pause: {}ms", miruBotDistinctsConfig.getWriteReadPauseMs());
        LOG.info("Runtime: {}ms", miruBotDistinctsConfig.getRuntimeMs());

        return new MiruBotDistinctsWorker(
                miruIngressEndpoint,
                miruBotDistinctsConfig,
                orderIdProvider,
                miruBotSchemaService,
                miruClientReader,
                miruClientWriter);
    }

    MiruBotBucketSnapshot genMiruBotBucketSnapshot() {
        return miruBotDistinctsWorker == null ? null : miruBotDistinctsWorker.genMiruBotBucketSnapshot();
    }

    public double getHealthPercentage() {
        return miruBotDistinctsWorker == null ? 1.0 : miruBotDistinctsWorker.getHealthPercentage();
    }

    public String getHealthDescription() {
        return miruBotDistinctsWorker == null ? "" : miruBotDistinctsWorker.getHealthDescription();
    }

}
