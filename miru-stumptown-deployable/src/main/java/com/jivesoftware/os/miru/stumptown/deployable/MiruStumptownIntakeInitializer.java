package com.jivesoftware.os.miru.stumptown.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.miru.api.MiruWriterEndpointConstants;
import com.jivesoftware.os.miru.stumptown.deployable.storage.MiruStumptownPayloadStorage;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import org.merlin.config.Config;
import org.merlin.config.defaults.IntDefault;
import org.merlin.config.defaults.StringDefault;

/**
 * @author jonathan.colt
 */
public class MiruStumptownIntakeInitializer {

    public interface MiruStumptownIntakeConfig extends Config {

        @StringDefault(MiruWriterEndpointConstants.INGRESS_PREFIX + MiruWriterEndpointConstants.ADD)
        String getMiruIngressEndpoint();

        @StringDefault("var/queues")
        String getPathToQueues();

        @IntDefault(1_000)
        int getMaxDrainSize();

        @IntDefault(1)
        int getNumberOfQueues();

        @IntDefault(24)
        int getNumberOfThreadsPerQueue();
    }

    MiruStumptownIntakeService initialize(boolean enabled,
        MiruStumptownIntakeConfig config,
        StumptownSchemaService stumptownSchemaService,
        LogMill logMill,
        ObjectMapper activityMapper,
        TenantAwareHttpClient<String> miruWriters,
        MiruStumptownPayloadStorage payloadStorage) {

        return new MiruStumptownIntakeService(enabled,
            stumptownSchemaService,
            logMill,
            config.getMiruIngressEndpoint(),
            activityMapper,
            miruWriters,
            payloadStorage);
    }
}
