package com.jivesoftware.os.miru.bot.deployable;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.miru.bot.deployable.MiruBotUniquesInitializer.MiruBotUniquesConfig;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

class MiruBotUniquesService implements MiruBotHealthPercent {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruBotUniquesConfig miruBotUniquesConfig;
    private final String miruIngressEndpoint;
    private final OrderIdProvider orderIdProvider;
    private final MiruBotSchemaService miruBotSchemaService;
    private final TenantAwareHttpClient<String> miruClientReader;
    private final TenantAwareHttpClient<String> miruClientWriter;

    private MiruBotUniquesWorker miruBotUniquesWorker;

    private final ScheduledExecutorService processor =
            Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder().setNameFormat("mirubot-uniques-%d").build());

    MiruBotUniquesService(String miruIngressEndpoint,
                          MiruBotUniquesConfig miruBotUniquesConfig,
                          OrderIdProvider orderIdProvider,
                          MiruBotSchemaService miruBotSchemaService,
                          TenantAwareHttpClient<String> miruClientReader,
                          TenantAwareHttpClient<String> miruClientWriter) {
        this.miruBotUniquesConfig = miruBotUniquesConfig;
        this.miruIngressEndpoint = miruIngressEndpoint;
        this.orderIdProvider = orderIdProvider;
        this.miruBotSchemaService = miruBotSchemaService;
        this.miruClientReader = miruClientReader;
        this.miruClientWriter = miruClientWriter;
    }

    void start() throws Exception {
        if (!miruBotUniquesConfig.getEnabled()) {
            LOG.warn("Not starting uniques service; not enabled.");
            return;
        }

        LOG.info("Enabled: {}", miruBotUniquesConfig.getEnabled());
        LOG.info("Read time range factor: {}ms", miruBotUniquesConfig.getReadTimeRangeFactorMs());
        LOG.info("Write hesitation factor: {}ms", miruBotUniquesConfig.getWriteHesitationFactorMs());
        LOG.info("Value size factor: {}", miruBotUniquesConfig.getValueSizeFactor());
        //LOG.info("Retry wait: {}", miruBotUniquesConfig.getRetryWaitMs());
        LOG.info("Birth rate factor: {}", miruBotUniquesConfig.getBirthRateFactor());
        LOG.info("Read frequency: {}", miruBotUniquesConfig.getReadFrequency());
        LOG.info("Batch write count factor: {}", miruBotUniquesConfig.getBatchWriteCountFactor());
        LOG.info("Batch write frequency: {}", miruBotUniquesConfig.getBatchWriteFrequency());
        LOG.info("Number of fields: {}", miruBotUniquesConfig.getNumberOfFields());
        LOG.info("Bot bucket seed: {}", miruBotUniquesConfig.getBotBucketSeed());
        LOG.info("Write read pause: {}ms", miruBotUniquesConfig.getWriteReadPauseMs());
        LOG.info("Runtime: {}ms", miruBotUniquesConfig.getRuntimeMs());

        miruBotUniquesWorker = createWithConfig(miruBotUniquesConfig);
        processor.submit(miruBotUniquesWorker);
    }

    public void stop() throws InterruptedException {
        processor.shutdownNow();
    }

    MiruBotUniquesWorker createWithConfig(MiruBotUniquesConfig miruBotUniquesConfig) {
        return new MiruBotUniquesWorker(
                miruIngressEndpoint,
                miruBotUniquesConfig,
                orderIdProvider,
                miruBotSchemaService,
                miruClientReader,
                miruClientWriter);
    }

    MiruBotBucketSnapshot genMiruBotBucketSnapshot() {
        return miruBotUniquesWorker.genMiruBotBucketSnapshot();
    }

    public double getHealthPercentage() {
        if (miruBotUniquesWorker == null) return 1.0;
        return miruBotUniquesWorker.getHealthPercentage();
    }

    public String getHealthDescription() {
        if (miruBotUniquesWorker == null) return "";
        return miruBotUniquesWorker.getHealthDescription();
    }

}
