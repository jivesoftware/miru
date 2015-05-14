package com.jivesoftware.os.miru.api.wal;

import org.merlin.config.Config;
import org.merlin.config.defaults.StringDefault;

/**
 *
 */
public interface MiruWALConfig extends Config {

    @StringDefault("rcvs")
    String getActivityWALType(); // rcvs or amza or fork

}
