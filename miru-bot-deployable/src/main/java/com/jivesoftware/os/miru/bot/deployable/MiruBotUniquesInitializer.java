package com.jivesoftware.os.miru.bot.deployable;

import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.miru.bot.deployable.MiruBotDistinctsInitializer.MiruBotDistinctsConfig;
import org.merlin.config.defaults.BooleanDefault;

class MiruBotUniquesInitializer {

    interface MiruBotUniquesConfig extends MiruBotDistinctsConfig {

        @Override
        @BooleanDefault(false)
        boolean getEnabled();

    }

    MiruBotUniquesService initialize(MiruBotConfig miruBotConfig,
        MiruBotUniquesConfig miruBotUniquesConfig,
        OrderIdProvider orderIdProvider,
        MiruBotSchemaService miruBotSchemaService,
        TenantAwareHttpClient<String> miruReader,
        TenantAwareHttpClient<String> miruWriter) {
        return new MiruBotUniquesService(
            miruBotConfig.getMiruIngressEndpoint(),
            miruBotUniquesConfig,
            orderIdProvider,
            miruBotSchemaService,
            miruReader,
            miruWriter);
    }

}
