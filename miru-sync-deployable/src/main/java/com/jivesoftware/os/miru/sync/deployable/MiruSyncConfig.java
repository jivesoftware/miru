package com.jivesoftware.os.miru.sync.deployable;

import com.jivesoftware.os.miru.api.sync.ActivityReadEventConverter;
import org.merlin.config.Config;
import org.merlin.config.defaults.BooleanDefault;
import org.merlin.config.defaults.ClassDefault;
import org.merlin.config.defaults.IntDefault;
import org.merlin.config.defaults.LongDefault;

public interface MiruSyncConfig extends Config {

    @BooleanDefault(false)
    boolean getSyncSenderEnabled();

    @IntDefault(60_000)
    int getSyncSenderSocketTimeout();

    @BooleanDefault(false)
    boolean getSyncReceiverEnabled();

    @IntDefault(16)
    int getSyncRingStripes();

    @IntDefault(4)
    int getSyncThreadCount();

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
}
