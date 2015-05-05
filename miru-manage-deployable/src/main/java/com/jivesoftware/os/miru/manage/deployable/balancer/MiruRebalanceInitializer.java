package com.jivesoftware.os.miru.manage.deployable.balancer;

import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.miru.api.topology.ReaderRequestHelpers;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;

public class MiruRebalanceInitializer {

    public MiruRebalanceDirector initialize(MiruClusterRegistry clusterRegistry,
        MiruWALClient miruWALClient,
        OrderIdProvider orderIdProvider,
        ReaderRequestHelpers readerRequestHelpers)
        throws Exception {

        return new MiruRebalanceDirector(clusterRegistry, miruWALClient, orderIdProvider, readerRequestHelpers);
    }
}
