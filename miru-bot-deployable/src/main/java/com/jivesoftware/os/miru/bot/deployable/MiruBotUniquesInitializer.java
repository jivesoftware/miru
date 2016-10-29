package com.jivesoftware.os.miru.bot.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.routing.bird.http.client.HttpResponseMapper;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import org.merlin.config.Config;
import org.merlin.config.defaults.BooleanDefault;

class MiruBotUniquesInitializer {

    interface MiruBotUniquesConfig extends Config {

        @BooleanDefault(false)
        boolean getEnabled();

    }

    MiruBotUniquesService initialize(MiruBotConfig miruBotConfig,
                                     MiruBotUniquesConfig miruBotUniquesConfig,
                                     ObjectMapper objectMapper,
                                     HttpResponseMapper responseMapper,
                                     OrderIdProvider orderIdProvider,
                                     MiruBotSchemaService miruBotSchemaService,
                                     TenantAwareHttpClient<String> miruReader,
                                     TenantAwareHttpClient<String> miruWriter) {
        return new MiruBotUniquesService(
                miruBotUniquesConfig,
                miruBotConfig.getMiruIngressEndpoint(),
                objectMapper,
                responseMapper,
                orderIdProvider,
                miruBotSchemaService,
                miruReader,
                miruWriter);
    }

}
