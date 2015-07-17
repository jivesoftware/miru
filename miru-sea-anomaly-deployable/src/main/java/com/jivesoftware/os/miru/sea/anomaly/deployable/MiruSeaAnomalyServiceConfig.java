package com.jivesoftware.os.miru.sea.anomaly.deployable;

import com.jivesoftware.os.miru.api.MiruWriterEndpointConstants;
import org.merlin.config.Config;
import org.merlin.config.defaults.StringDefault;

/**
 *
 * @author jonathan.colt
 */
public interface MiruSeaAnomalyServiceConfig extends Config {

    @StringDefault(MiruWriterEndpointConstants.INGRESS_PREFIX + MiruWriterEndpointConstants.ADD)
    public String getMiruIngressEndpoint();

    @StringDefault("unspecifiedHost:0")
    public String getMiruWriterHosts();

    //@StringDefault("unspecifiedHost:0")
    @StringDefault("soa-prime-data8.phx1.jivehosted.com:10004")
    public String getMiruReaderHosts();

}
