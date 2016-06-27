package com.jivesoftware.os.miru.anomaly.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.miru.api.MiruWriterEndpointConstants;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import org.merlin.config.Config;
import org.merlin.config.defaults.IntDefault;
import org.merlin.config.defaults.StringDefault;

/**
 * @author jonathan.colt
 */
public class MiruAnomalyIntakeInitializer {

    public interface MiruAnomalyIntakeConfig extends Config {

        @StringDefault(MiruWriterEndpointConstants.INGRESS_PREFIX + MiruWriterEndpointConstants.ADD)
        public String getMiruIngressEndpoint();

        @StringDefault("var/queues")
        String getPathToQueues();

        @IntDefault(1_000)
        int getMaxDrainSize();

        @IntDefault(1)
        int getNumberOfQueues();

        @IntDefault(24)
        int getNumberOfThreadsPerQueue();
    }

    MiruAnomalyIntakeService initialize(boolean enabled,
        MiruAnomalyIntakeConfig config,
        AnomalySchemaService anomalySchemaService,
        SampleTrawl logMill,
        ObjectMapper intakeMapper,
        TenantAwareHttpClient<String> miruWriterClient) {

        return new MiruAnomalyIntakeService(enabled, anomalySchemaService, logMill, config.getMiruIngressEndpoint(), intakeMapper, miruWriterClient);
    }
}
