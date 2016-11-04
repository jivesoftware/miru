package com.jivesoftware.os.miru.sync.deployable;

import org.merlin.config.Config;
import org.merlin.config.defaults.BooleanDefault;
import org.merlin.config.defaults.IntDefault;
import org.merlin.config.defaults.LongDefault;
import org.merlin.config.defaults.StringDefault;

public interface MiruSyncConfig extends Config {

    @BooleanDefault(false)
    boolean getSyncSenderEnabled();

    @StringDefault("")
    String getSyncSenderWhitelist();

    @StringDefault("")
    String getSyncSenderSchemeHostPort();

    @IntDefault(60_000)
    int getSyncSenderSocketTimeout();

    @BooleanDefault(false)
    boolean getSyncReceiverEnabled();

    @IntDefault(16)
    int getSyncRingStripes();

    @IntDefault(4)
    int getSyncThreadCount();

    @LongDefault(15_000)
    long getSyncIntervalMillis();

    @IntDefault(10_000)
    int getSyncBatchSize();

    @LongDefault(60_000)
    long getForwardSyncDelayMillis();

    @IntDefault(16)
    int getAmzaCallerThreadPoolSize();

    @LongDefault(60_000)
    long getAmzaAwaitLeaderElectionForNMillis();

    @StringDefault("")
    String getSyncSenderOAuthConsumerKey();

    @StringDefault("")
    String getSyncSenderOAuthConsumerSecret();

    @StringDefault("")
    String getSyncSenderOAuthConsumerMethod();
}
