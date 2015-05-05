package com.jivesoftware.os.miru.reader;

import com.jivesoftware.os.miru.api.MiruReader;
import com.jivesoftware.os.miru.reader.cluster.MiruClusterReader;
import org.merlin.config.Config;
import org.merlin.config.defaults.BooleanDefault;
import org.merlin.config.defaults.ClassDefault;
import org.merlin.config.defaults.IntDefault;
import org.merlin.config.defaults.StringDefault;

public interface MiruHttpClientReaderConfig extends Config {

    // This is a default/override used only by naive in-process implementations. Real implementations should read from the registry
    @StringDefault("")
    String getDefaultHostAddresses();

    @IntDefault(-1)
    Integer getSocketTimeoutInMillis();

    @IntDefault(100)
    Integer getMaxConnections();

    @ClassDefault(MiruClusterReader.class)
    Class<? extends MiruReader> getReaderClass();

    @IntDefault(4)
    Integer getWarmOfflineHostExecutorCount();

    @BooleanDefault(true)
    Boolean getUserNodeAffinity();
}
