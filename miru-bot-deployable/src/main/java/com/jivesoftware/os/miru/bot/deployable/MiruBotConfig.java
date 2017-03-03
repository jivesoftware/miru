package com.jivesoftware.os.miru.bot.deployable;

import com.jivesoftware.os.miru.api.MiruWriterEndpointConstants;
import org.merlin.config.Config;
import org.merlin.config.defaults.IntDefault;
import org.merlin.config.defaults.LongDefault;
import org.merlin.config.defaults.StringDefault;

interface MiruBotConfig extends Config {

    @IntDefault(10)
    int getDeadAfterNErrors();

    @LongDefault(10_000L)
    long getCheckDeadEveryNMillis();

    @LongDefault(10_000L)
    long getRefreshConnectionsAfterNMillis();

    @IntDefault(5_000)
    int getHealthInterval();

    @IntDefault(100)
    int getHealthSampleWindow();

    @StringDefault(MiruWriterEndpointConstants.INGRESS_PREFIX + MiruWriterEndpointConstants.ADD)
    String getMiruIngressEndpoint();

}
