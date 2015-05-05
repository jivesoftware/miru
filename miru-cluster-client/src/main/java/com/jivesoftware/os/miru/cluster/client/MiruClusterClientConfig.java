package com.jivesoftware.os.miru.cluster.client;

import org.merlin.config.Config;
import org.merlin.config.defaults.IntDefault;
import org.merlin.config.defaults.StringDefault;

public interface MiruClusterClientConfig extends Config {

    @IntDefault(10_000)
    Integer getSocketTimeoutInMillis();

    @IntDefault(100)
    Integer getMaxConnections();

    @StringDefault("unknownhost:port,unknownhost:port")
    String getManageHostAddresses();

    void setManageHostAddresses(String hostPort);
}
