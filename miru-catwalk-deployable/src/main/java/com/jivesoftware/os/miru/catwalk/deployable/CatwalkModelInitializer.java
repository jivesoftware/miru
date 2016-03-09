package com.jivesoftware.os.miru.catwalk.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.amza.api.PartitionClientProvider;
import com.jivesoftware.os.miru.api.MiruStats;

/**
 *
 */
public class CatwalkModelInitializer {

    public CatwalkModelService initialize(PartitionClientProvider clientProvider,
        MiruStats stats,
        ObjectMapper mapper)
        throws Exception {

        return new CatwalkModelService(clientProvider, stats, mapper);
    }
}
