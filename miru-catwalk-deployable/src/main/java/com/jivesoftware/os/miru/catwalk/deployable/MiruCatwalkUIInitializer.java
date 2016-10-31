package com.jivesoftware.os.miru.catwalk.deployable;

import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.catwalk.deployable.region.MiruHeaderRegion;
import com.jivesoftware.os.miru.catwalk.deployable.region.MiruInspectRegion;
import com.jivesoftware.os.miru.ui.MiruAdminRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.routing.bird.shared.TenantRoutingProvider;

public class MiruCatwalkUIInitializer {

    public MiruCatwalkUIService initialize(String cluster,
        int instance,
        MiruSoyRenderer renderer,
        MiruStats stats,
        TenantRoutingProvider tenantRoutingProvider,
        CatwalkModelService catwalkModelService)
        throws Exception {

        return new MiruCatwalkUIService(
            renderer,
            new MiruHeaderRegion(cluster, instance, "soy.miru.chrome.headerRegion", renderer, tenantRoutingProvider),
            new MiruAdminRegion("soy.miru.page.adminRegion", renderer, stats),
            new MiruInspectRegion("soy.miru.page.inspectRegion", renderer, catwalkModelService));
    }
}
