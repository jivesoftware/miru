package com.jivesoftware.os.miru.siphon.deployable;

import com.jivesoftware.os.miru.api.MiruWriterEndpointConstants;
import org.merlin.config.Config;
import org.merlin.config.defaults.BooleanDefault;
import org.merlin.config.defaults.IntDefault;
import org.merlin.config.defaults.LongDefault;
import org.merlin.config.defaults.StringDefault;

/**
 * @author jonathan.colt
 */
public interface MiruSiphonConfig extends Config {

    @StringDefault(MiruWriterEndpointConstants.INGRESS_PREFIX + MiruWriterEndpointConstants.ADD)
    String getMiruIngressEndpoint();

    @StringDefault("")
    String getSiphonPackages();

    @IntDefault(1024)
    int getAmzaCallerThreadPoolSize();

    @LongDefault(60_000)
    long getAmzaAwaitLeaderElectionForNMillis();

    @BooleanDefault(false)
    boolean getAquariumUseSolutionLog();

    @IntDefault(1024)
    int getSiphonStripingCount();

    @LongDefault(10_000)
    long getEnsureSiphonersIntervalMillis();
}
