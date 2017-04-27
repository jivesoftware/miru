package com.jivesoftware.os.miru.siphon.deployable;

import com.jivesoftware.os.miru.api.MiruWriterEndpointConstants;
import org.merlin.config.Config;
import org.merlin.config.defaults.BooleanDefault;
import org.merlin.config.defaults.StringDefault;

/**
 *
 * @author jonathan.colt
 */
public interface WikiMiruServiceConfig extends Config {

    @StringDefault(MiruWriterEndpointConstants.INGRESS_PREFIX + MiruWriterEndpointConstants.ADD)
    public String getMiruIngressEndpoint();

    @BooleanDefault(true)
    boolean getIngressEnabled();

}
