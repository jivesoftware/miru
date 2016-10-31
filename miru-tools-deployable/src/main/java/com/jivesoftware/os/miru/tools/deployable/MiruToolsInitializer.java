package com.jivesoftware.os.miru.tools.deployable;

import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.tools.deployable.region.MiruHeaderRegion;
import com.jivesoftware.os.miru.ui.MiruAdminRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.routing.bird.shared.TenantRoutingProvider;

public class MiruToolsInitializer {

    public MiruToolsService initialize(MiruStats miruStats,
        String cluster,
        int instance,
        MiruSoyRenderer renderer,
        TenantRoutingProvider tenantRoutingProvider) throws Exception {

        return new MiruToolsService(
            renderer,
            new MiruHeaderRegion(cluster, instance, "soy.miru.chrome.headerRegion", renderer, tenantRoutingProvider),
            new MiruAdminRegion("soy.miru.page.adminRegion", renderer, miruStats));
    }
}
