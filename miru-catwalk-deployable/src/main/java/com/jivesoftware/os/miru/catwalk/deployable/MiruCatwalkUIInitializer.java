package com.jivesoftware.os.miru.catwalk.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.catwalk.deployable.region.MiruAdminRegion;
import com.jivesoftware.os.miru.catwalk.deployable.region.MiruHeaderRegion;
import com.jivesoftware.os.miru.catwalk.deployable.region.MiruSomethingRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.routing.bird.shared.TenantRoutingProvider;

public class MiruCatwalkUIInitializer {

    public MiruCatwalkUIService initialize(String cluster,
        int instance,
        MiruSoyRenderer renderer,
        MiruStats stats,
        TenantRoutingProvider tenantRoutingProvider,
        ObjectMapper mapper)
        throws Exception {

        return new MiruCatwalkUIService(
            renderer,
            new MiruHeaderRegion(cluster, instance, "soy.miru.chrome.headerRegion", renderer, tenantRoutingProvider),
            new MiruAdminRegion("soy.miru.page.adminRegion", renderer, stats),
            new MiruSomethingRegion("soy.miru.page.somethingRegion", renderer));
    }
}
