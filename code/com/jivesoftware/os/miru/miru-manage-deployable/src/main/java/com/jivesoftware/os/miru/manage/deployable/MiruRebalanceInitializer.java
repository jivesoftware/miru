package com.jivesoftware.os.miru.manage.deployable;

import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;

public class MiruRebalanceInitializer {

    public MiruRebalanceDirector initialize(MiruClusterRegistry clusterRegistry,
        OrderIdProvider orderIdProvider,
        ReaderRequestHelpers readerRequestHelpers)
        throws Exception {

        return new MiruRebalanceDirector(clusterRegistry, orderIdProvider, readerRequestHelpers);
    }
}
