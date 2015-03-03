package com.jivesoftware.os.miru.cluster.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.miru.api.topology.MiruClusterClient;

public class MiruClusterClientInitializer {

    public MiruClusterClient initialize(MiruClusterClientConfig config, ObjectMapper mapper) throws Exception {

        RequestHelper[] requestHelpers = MiruRequestHelperUtil.buildRequestHelpers(config.getManageHostAddresses(),
            mapper,
            config.getSocketTimeoutInMillis().intValue(),
            config.getMaxConnections());
        return new MiruHttpClusterClient(requestHelpers);
    }
}
