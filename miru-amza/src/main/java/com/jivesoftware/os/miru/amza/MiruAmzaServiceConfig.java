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

    @StringDefault("225.4.5.7")
    String getAmzaDiscoveryGroup();

    void setAmzaDiscoveryGroup(String amzaDiscoveryGroup);

    @IntDefault(-1)
    int getAmzaDiscoveryPort();

    void setAmzaDiscoveryPort(int amzaDiscoveryPort);

    @LongDefault(60_000)
    long getCheckIfCompactionIsNeededIntervalInMillis();

    @IntDefault(60_000)
    int getDeltaStripeCompactionIntervalInMillis();

    @IntDefault(1000)
    int getTakeFromNeighborsIntervalInMillis();

    void setTakeFromNeighborsIntervalInMillis(int takeFromNeighborsIntervalInMillis);

    @LongDefault(1 * 24 * 60 * 60 * 1000L)
    long getCompactTombstoneIfOlderThanNMillis();

    @LongDefault(60_000L)
    long getMinimumRangeCheckIntervalInMillis();

    @IntDefault(100)
    int getCorruptionParanoiaFactor();

    @IntDefault(1_000_000)
    int getMaxUpdatesBeforeDeltaStripeCompaction();

    @IntDefault(1)
    int getNumberOfCompactorThreads();

    @IntDefault(24)
    int getNumberOfTakerThreads();

    @IntDefault(24)
    int getNumberOfAckerThreads();

    @IntDefault(1)
    int getTakeFromFactor();

    @BooleanDefault(false)
    boolean getHardFsync();
}
