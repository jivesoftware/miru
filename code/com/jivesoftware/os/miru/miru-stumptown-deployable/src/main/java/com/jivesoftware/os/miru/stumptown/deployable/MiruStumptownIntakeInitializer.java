/*
 * Copyright 2015 jonathan.colt.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jivesoftware.os.miru.stumptown.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.miru.api.MiruWriterEndpointConstants;
import com.jivesoftware.os.miru.stumptown.deployable.storage.MiruStumptownPayloads;
import com.jivesoftware.os.upena.tenant.routing.http.client.TenantAwareHttpClient;
import org.merlin.config.Config;
import org.merlin.config.defaults.IntDefault;
import org.merlin.config.defaults.StringDefault;

/**
 * @author jonathan.colt
 */
public class MiruStumptownIntakeInitializer {

    public interface MiruStumptownIntakeConfig extends Config {

        @StringDefault(MiruWriterEndpointConstants.INGRESS_PREFIX + MiruWriterEndpointConstants.ADD)
        public String getMiruIngressEndpoint();

        @StringDefault("var/queues")
        String getPathToQueues();

        @IntDefault(1_000)
        int getMaxDrainSize();

    }

    MiruStumptownIntakeService initialize(MiruStumptownIntakeConfig config,
        StumptownSchemaService stumptownSchemaService,
        LogMill logMill,
        ObjectMapper activityMapper,
        TenantAwareHttpClient<String> miruWriters,
        MiruStumptownPayloads activityPayloads) {

        return new MiruStumptownIntakeService(stumptownSchemaService,
            logMill,
            config.getMiruIngressEndpoint(),
            activityMapper,
            miruWriters,
            activityPayloads);
    }
}
