package com.jivesoftware.os.miru.sync.deployable;

import com.jivesoftware.os.miru.api.sync.ActivityReadEventConverter;
import org.merlin.config.Config;
import org.merlin.config.defaults.BooleanDefault;
import org.merlin.config.defaults.ClassDefault;
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

    @LongDefault(1_000L * 60 * 60 * 24 * 90) // 90 days
    long getReverseSyncMaxAgeMillis();

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

    @BooleanDefault(false)
    boolean getSyncSenderAllowSelfSignedCerts();

    @ClassDefault(NoOpActivityReadEventConverter.class)
    Class<? extends ActivityReadEventConverter> getSyncReceiverActivityReadEventConverterClass();

    @BooleanDefault(false)
    boolean getUseClientSolutionLog();

    @IntDefault(10_000)
    int getCopyBatchSize();
}
