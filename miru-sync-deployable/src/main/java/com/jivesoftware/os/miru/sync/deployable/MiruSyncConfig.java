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

    @IntDefault(60_000)
    int getSyncSenderSocketTimeout();

    @BooleanDefault(false)
    boolean getSyncReceiverEnabled();

    @IntDefault(128)
    int getSyncRingStripes();

    @IntDefault(24)
    int getSyncSendersThreadCount();

    @IntDefault(16)
    int getAmzaCallerThreadPoolSize();

    @LongDefault(60_000)
    long getAmzaAwaitLeaderElectionForNMillis();

    @ClassDefault(NoOpActivityReadEventConverter.class)
    Class<? extends ActivityReadEventConverter> getSyncReceiverActivityReadEventConverterClass();

    @BooleanDefault(false)
    boolean getUseClientSolutionLog();

    @IntDefault(10_000)
    int getCopyBatchSize();

    @StringDefault("")
    String getSyncLoopback();

    @LongDefault(1_000L * 60 * 60 * 24 * 30)
    long getSyncLoopbackDurationMillis();
}
