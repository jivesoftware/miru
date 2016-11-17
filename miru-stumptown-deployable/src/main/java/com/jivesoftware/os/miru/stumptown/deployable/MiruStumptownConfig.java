package com.jivesoftware.os.miru.stumptown.deployable;

import org.merlin.config.Config;
import org.merlin.config.defaults.IntDefault;
import org.merlin.config.defaults.LongDefault;

interface MiruStumptownConfig extends Config {

    @IntDefault(10)
    int getDeadAfterNErrors();

    @LongDefault(10_000L)
    long getCheckDeadEveryNMillis();

    @LongDefault(10_000L)
    long getRefreshConnectionsAfterNMillis();

    @LongDefault(30_000L)
    long getAwaitLeaderElectionForNMillis();

    @LongDefault(5_000L)
    long getHealthIntervalNMillis();

    @IntDefault(100)
    int getHealthSampleWindow();

}
