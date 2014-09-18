package com.jivesoftware.os.miru.reader;

import org.merlin.config.Config;
import org.merlin.config.defaults.IntDefault;
import org.merlin.config.defaults.StringDefault;

public interface MiruHttpClientConfigReaderConfig extends Config {

    @StringDefault("localhost")
    public String getHost();

    @IntDefault(49_600)
    public Integer getPort();

    @IntDefault(-1)
    public Integer getSocketTimeoutInMillis();

    @IntDefault(100)
    public Integer getMaxConnections();

}
