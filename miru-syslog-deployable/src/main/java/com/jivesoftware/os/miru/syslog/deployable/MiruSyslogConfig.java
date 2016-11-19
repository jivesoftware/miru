package com.jivesoftware.os.miru.syslog.deployable;

import org.merlin.config.Config;
import org.merlin.config.defaults.IntDefault;
import org.merlin.config.defaults.LongDefault;
import org.merlin.config.defaults.StringDefault;

interface MiruSyslogConfig extends Config {

    @LongDefault(10_000L)
    long getRefreshConnectionsAfterNMillis();

    @LongDefault(5_000L)
    long getHealthIntervalNMillis();

    @IntDefault(100)
    int getHealthSampleWindow();

    @StringDefault("")
    String getTenant();

}
