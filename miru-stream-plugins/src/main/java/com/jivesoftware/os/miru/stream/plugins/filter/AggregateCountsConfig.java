package com.jivesoftware.os.miru.stream.plugins.filter;

import org.merlin.config.Config;
import org.merlin.config.defaults.StringDefault;

/**
 * @author jonathan.colt
 */
public interface AggregateCountsConfig extends Config {

    @StringDefault("")
    String getVerboseStreamIds();

}
