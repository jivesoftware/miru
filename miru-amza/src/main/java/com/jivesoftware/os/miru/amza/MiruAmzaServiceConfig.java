package com.jivesoftware.os.miru.amza;

import org.merlin.config.Config;
import org.merlin.config.defaults.BooleanDefault;
import org.merlin.config.defaults.IntDefault;
import org.merlin.config.defaults.LongDefault;
import org.merlin.config.defaults.StringDefault;

/**
 *
 */
public interface MiruAmzaServiceConfig extends Config {

    @StringDefault("./var/amza/default/data/")
    String getWorkingDirectories();

    void setWorkingDirectories(String dir);

    @StringDefault("./var/amza/default/index/")
    String getIndexDirectories();

    void setIndexDirectories(String dir);

    @IntDefault(-1)
    int getAmzaDiscoveryPort();

    void setAmzaDiscoveryPort(int amzaDiscoveryPort);

    @IntDefault(1000)
    int getApplyReplicasIntervalInMillis();

    @LongDefault(60_000)
    long getCheckIfCompactionIsNeededIntervalInMillis();

    @IntDefault(60_000)
    int getDeltaStripeCompactionIntervalInMillis();

    @IntDefault(1000)
    int getResendReplicasIntervalInMillis();

    @IntDefault(1000)
    int getTakeFromNeighborsIntervalInMillis();

    @LongDefault(1 * 24 * 60 * 60 * 1000L)
    long getCompactTombstoneIfOlderThanNMillis();

    @LongDefault(60_000L)
    long getMinimumRangeCheckIntervalInMillis();

    @IntDefault(100)
    int getCorruptionParanoiaFactor();

    @IntDefault(1_000_000)
    int getMaxUpdatesBeforeDeltaStripeCompaction();

    @IntDefault(1)
    int getNumberOfApplierThreads();

    @IntDefault(1)
    int getNumberOfCompactorThreads();

    @IntDefault(24)
    int getNumberOfReplicatorThreads();

    @IntDefault(8)
    int getNumberOfResendThreads();

    @IntDefault(24)
    int getNumberOfTakerThreads();

    @IntDefault(1)
    int getReplicationFactor();

    @IntDefault(1)
    int getTakeFromFactor();

    @BooleanDefault(false)
    boolean getHardFsync();
}
