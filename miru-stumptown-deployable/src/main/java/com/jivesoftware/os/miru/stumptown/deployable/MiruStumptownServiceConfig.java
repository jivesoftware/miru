package com.jivesoftware.os.miru.stumptown.deployable;

import com.jivesoftware.os.miru.api.MiruWriterEndpointConstants;
import com.jivesoftware.os.rcvs.api.RowColumnValueStoreInitializer;
import com.jivesoftware.os.rcvs.api.RowColumnValueStoreProvider;
import org.merlin.config.Config;
import org.merlin.config.defaults.ClassDefault;
import org.merlin.config.defaults.StringDefault;

/**
 *
 * @author jonathan.colt
 */
public interface MiruStumptownServiceConfig extends Config {

    @StringDefault(MiruWriterEndpointConstants.INGRESS_PREFIX + MiruWriterEndpointConstants.ADD)
    public String getMiruIngressEndpoint();

    @StringDefault("unspecifiedHost:0")
    public String getMiruWriterHosts();

    @StringDefault("unspecifiedHost:0")
    public String getMiruReaderHosts();

    @StringDefault("hbase") // hbase or amza
    public String getPayloadStorageSolution();

    @ClassDefault(IllegalStateException.class)
    <C extends Config, I extends RowColumnValueStoreInitializer<E>, E extends Exception> Class<RowColumnValueStoreProvider<C, I, E>>
    getRowColumnValueStoreProviderClass();

}
