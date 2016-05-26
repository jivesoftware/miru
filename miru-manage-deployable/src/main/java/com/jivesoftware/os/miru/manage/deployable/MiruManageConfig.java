package com.jivesoftware.os.miru.manage.deployable;

import org.merlin.config.Config;
import org.merlin.config.defaults.BooleanDefault;

/**
 *
 * @author jonathan.colt
 */
public interface MiruManageConfig extends Config {

    @BooleanDefault(false)
    boolean getRackAwareElections();

}
