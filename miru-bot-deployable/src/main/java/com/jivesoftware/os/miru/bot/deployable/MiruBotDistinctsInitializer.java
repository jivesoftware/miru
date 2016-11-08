package com.jivesoftware.os.miru.bot.deployable;

import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import org.merlin.config.Config;
import org.merlin.config.defaults.BooleanDefault;
import org.merlin.config.defaults.IntDefault;
import org.merlin.config.defaults.LongDefault;

class MiruBotDistinctsInitializer {

    interface MiruBotDistinctsConfig extends Config {

        @BooleanDefault(false)
        boolean getEnabled();

        @IntDefault(10800000)
        int getReadTimeRangeFactor();
        void setReadTimeRangeFactor(int value);

        @IntDefault(10)
        int getWriteHesitationFactor();
        void setWriteHesitationFactor(int value);

        @IntDefault(10)
        int getValueSizeFactor();
        void setValueSizeFactor(int value);

        @LongDefault(20_000L)
        long getFailureRetryWaitMs();
        void setFailureRetryWaitMs(long value);

        @IntDefault(500)
        int getBirthRateFactor();
        void setBirthRateFactor(int value);

        @IntDefault(1_000)
        int getReadFrequency();
        void setReadFrequency(int value);

        @IntDefault(100)
        int getBatchWriteCountFactor();
        void setBatchWriteCountFactor(int value);

        @IntDefault(10)
        int getBatchWriteFrequency();
        void setBatchWriteFrequency(int value);

        @IntDefault(4)
        int getNumberOfFields();
        void setNumberOfFields(int value);

        @IntDefault(5)
        int getBotBucketSeed();
        void setBotBucketSeed(int value);

        @LongDefault(10_000L)
        long getWriteReadPauseMs();
        void setWriteReadPauseMs(long value);

        @LongDefault(Long.MAX_VALUE)
        long getRuntimeMs();
        void setRuntimeMs(long value);

    }

    MiruBotDistinctsService initialize(MiruBotConfig miruBotConfig,
                                       MiruBotDistinctsConfig miruBotDistinctsConfig,
                                       OrderIdProvider orderIdProvider,
                                       MiruBotSchemaService miruBotSchemaService,
                                       TenantAwareHttpClient<String> miruReader,
                                       TenantAwareHttpClient<String> miruWriter) {
        return new MiruBotDistinctsService(
                miruBotConfig.getMiruIngressEndpoint(),
                miruBotDistinctsConfig,
                orderIdProvider,
                miruBotSchemaService,
                miruReader,
                miruWriter);
    }

}
