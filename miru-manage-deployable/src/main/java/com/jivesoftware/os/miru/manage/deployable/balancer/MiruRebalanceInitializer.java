package com.jivesoftware.os.miru.manage.deployable.balancer;

import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.routing.bird.http.client.HttpResponseMapper;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.routing.bird.shared.TenantsServiceConnectionDescriptorProvider;

public class MiruRebalanceInitializer {

    public MiruRebalanceDirector initialize(MiruClusterRegistry clusterRegistry,
        MiruWALClient miruWALClient,
        OrderIdProvider orderIdProvider,
        TenantAwareHttpClient<String> readerClient,
        HttpResponseMapper responseMapper,
        TenantsServiceConnectionDescriptorProvider<String> readerConnectionDescriptorsProvider) throws Exception {

        return new MiruRebalanceDirector(clusterRegistry, miruWALClient, orderIdProvider, readerClient, responseMapper, readerConnectionDescriptorsProvider);
    }
}
