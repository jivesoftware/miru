package com.jivesoftware.os.miru.manage.deployable;

import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.cluster.client.ReaderRequestHelpers;
import com.jivesoftware.os.miru.wal.lookup.MiruActivityLookupTable;

public class MiruRebalanceInitializer {

    public MiruRebalanceDirector initialize(MiruClusterRegistry clusterRegistry,
        MiruActivityLookupTable activityLookupTable,
        OrderIdProvider orderIdProvider,
        ReaderRequestHelpers readerRequestHelpers)
        throws Exception {

        return new MiruRebalanceDirector(clusterRegistry, activityLookupTable, orderIdProvider, readerRequestHelpers);
    }
}
