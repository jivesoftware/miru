package com.jivesoftware.os.wiki.miru.deployable.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import java.io.IOException;

/**
 *
 * @author jonathan.colt
 */
public class WikiMiruPayloadsAmzaIntializer {

    public WikiMiruPayloadStorage initialize(String nameSpace,
        TenantAwareHttpClient<String> httpClient,
        long awaitLeaderElectionForNMillis,
        ObjectMapper mapper) throws IOException {

        return new WikiMiruPayloadsAmza(nameSpace, mapper, httpClient, awaitLeaderElectionForNMillis);
    }
}
