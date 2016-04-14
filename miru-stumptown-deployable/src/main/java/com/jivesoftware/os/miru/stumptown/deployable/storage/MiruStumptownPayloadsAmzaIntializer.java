package com.jivesoftware.os.miru.stumptown.deployable.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import java.io.IOException;

/**
 *
 * @author jonathan.colt
 */
public class MiruStumptownPayloadsAmzaIntializer {

    public MiruStumptownPayloadStorage initialize(String nameSpace,
        TenantAwareHttpClient<String> httpClient,
        long awaitLeaderElectionForNMillis,
        ObjectMapper mapper) throws IOException {

        return new MiruStumptownPayloadsAmza(nameSpace, mapper, httpClient, awaitLeaderElectionForNMillis);
    }
}
