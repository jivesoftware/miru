package com.jivesoftware.os.miru.sea.anomaly.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.miru.api.MiruWriterEndpointConstants;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import org.merlin.config.Config;
import org.merlin.config.defaults.IntDefault;
import org.merlin.config.defaults.StringDefault;

/**
 * @author jonathan.colt
 */
public class MiruSeaAnomalyIntakeInitializer {

    public interface MiruSeaAnomalyIntakeConfig extends Config {

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

    MiruSeaAnomalyIntakeService initialize(MiruSeaAnomalyIntakeConfig config,
        SeaAnomalySchemaService seaAnomalySchemaService,
        SampleTrawl logMill,
        ObjectMapper intakeMapper,
        TenantAwareHttpClient<String> miruWriterClient) {

        return new MiruSeaAnomalyIntakeService(seaAnomalySchemaService, logMill, config.getMiruIngressEndpoint(), intakeMapper, miruWriterClient);
    }
}
