package com.jivesoftware.os.miru.anomaly.deployable;

import org.merlin.config.Config;
import org.merlin.config.defaults.BooleanDefault;

/**
 *
 * @author jonathan.colt
 */
public interface MiruAnomalyServiceConfig extends Config {

    @BooleanDefault(true)
    boolean getIngressEnabled();

}
