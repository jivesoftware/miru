package com.jivesoftware.os.miru.cluster.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.jive.utils.http.client.rest.ResponseMapper;
import com.jivesoftware.os.miru.api.topology.MiruClusterClient;
import com.jivesoftware.os.upena.tenant.routing.http.client.TenantAwareHttpClient;

public class MiruClusterClientInitializer {

    public MiruClusterClient initialize(TenantAwareHttpClient<String> client, ObjectMapper mapper) throws Exception {

        return new MiruHttpClusterClient(client, mapper, new ResponseMapper(mapper));
    }
}
