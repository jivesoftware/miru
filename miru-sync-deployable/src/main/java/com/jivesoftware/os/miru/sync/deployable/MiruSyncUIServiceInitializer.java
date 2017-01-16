package com.jivesoftware.os.miru.sync.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.sync.deployable.region.MiruHeaderRegion;
import com.jivesoftware.os.miru.sync.deployable.region.MiruStatusFocusRegion;
import com.jivesoftware.os.miru.sync.deployable.region.MiruStatusRegion;
import com.jivesoftware.os.miru.ui.MiruAdminRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.routing.bird.shared.TenantRoutingProvider;

public class MiruSyncUIServiceInitializer {

    public MiruSyncUIService initialize(String cluster,
        int instance,
        MiruSoyRenderer renderer,
        MiruSyncSenders<?, ?> syncSenders,
        MiruStats miruStats,
        TenantRoutingProvider tenantRoutingProvider,
        ObjectMapper mapper)
        throws Exception {

        return new MiruSyncUIService(
            renderer,
            new MiruHeaderRegion(cluster, instance, "soy.miru.chrome.headerRegion", renderer, tenantRoutingProvider),
            new MiruAdminRegion("soy.miru.page.adminRegion", renderer, miruStats),
            new MiruStatusRegion("soy.miru.page.statusRegion", renderer,
                new MiruStatusFocusRegion("soy.miru.page.statusFocusRegion", renderer, syncSenders, mapper)));
    }
}
