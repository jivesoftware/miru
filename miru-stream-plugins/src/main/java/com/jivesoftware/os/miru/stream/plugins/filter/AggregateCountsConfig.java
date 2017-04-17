package com.jivesoftware.os.miru.stream.plugins.filter;

import org.merlin.config.Config;
import org.merlin.config.defaults.BooleanDefault;
import org.merlin.config.defaults.DoubleDefault;
import org.merlin.config.defaults.FloatDefault;
import org.merlin.config.defaults.IntDefault;
import org.merlin.config.defaults.LongDefault;
import org.merlin.config.defaults.StringDefault;

/**
 * @author jonathan.colt
 */
public interface AggregateCountsConfig extends Config {

    @StringDefault("")
    String getVerboseStreamIds();
}
