package com.jivesoftware.os.miru.bot.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.routing.bird.http.client.HttpResponseMapper;
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
        int getReadTimeRangeFactorMs();

        @IntDefault(10)
        int getWriteHesitationFactorMs();

        @IntDefault(10)
        int getValueSizeFactor();

        //@IntDefault(5_000)
        //int getRetryWaitMs();

        @IntDefault(500)
        int getBirthRateFactor();

        @IntDefault(1_000)
        int getReadFrequency();

        @IntDefault(100)
        int getBatchWriteCountFactor();

        @IntDefault(10)
        int getBatchWriteFrequency();

        @IntDefault(4)
        int getNumberOfFields();

        @IntDefault(10)
        int getBotBucketSeed();

        @LongDefault(5_000L)
        long getWriteReadPauseMs();

    }

    MiruBotDistinctsService initialize(MiruBotConfig miruBotConfig,
                                       MiruBotDistinctsConfig miruBotDistinctsConfig,
                                       ObjectMapper objectMapper,
                                       HttpResponseMapper responseMapper,
                                       OrderIdProvider orderIdProvider,
                                       MiruBotSchemaService miruBotSchemaService,
                                       TenantAwareHttpClient<String> miruReader,
                                       TenantAwareHttpClient<String> miruWriter) {
        return new MiruBotDistinctsService(
                miruBotConfig.getMiruIngressEndpoint(),
                miruBotDistinctsConfig,
                objectMapper,
                responseMapper,
                orderIdProvider,
                miruBotSchemaService,
                miruReader,
                miruWriter);
    }

}
