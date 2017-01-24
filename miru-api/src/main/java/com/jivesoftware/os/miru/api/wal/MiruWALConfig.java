package com.jivesoftware.os.miru.api.wal;

import org.merlin.config.Config;
import org.merlin.config.defaults.StringDefault;

/**
 *
 */
public interface MiruWALConfig extends Config {

    @StringDefault("rcvs")
    String getActivityWALType(); // one of: rcvs, rcvs_amza, amza_rcvs, amza

    @StringDefault("")
    String getTenantPartitionBlacklist();
}
