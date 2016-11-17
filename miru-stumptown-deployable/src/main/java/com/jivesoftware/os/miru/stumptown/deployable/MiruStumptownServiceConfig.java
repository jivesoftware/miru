package com.jivesoftware.os.miru.stumptown.deployable;

import org.merlin.config.Config;
import org.merlin.config.defaults.BooleanDefault;

/**
 *
 * @author jonathan.colt
 */
public interface MiruStumptownServiceConfig extends Config {

    @BooleanDefault(true)
    boolean getIngressEnabled();

}
