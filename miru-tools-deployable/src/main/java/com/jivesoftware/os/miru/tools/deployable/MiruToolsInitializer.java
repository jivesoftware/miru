package com.jivesoftware.os.miru.tools.deployable;

import com.jivesoftware.os.miru.tools.deployable.region.MiruHeaderRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.routing.bird.shared.TenantRoutingProvider;

public class MiruToolsInitializer {

    public MiruToolsService initialize(MiruSoyRenderer renderer, TenantRoutingProvider tenantRoutingProvider) throws Exception {

        return new MiruToolsService(
            renderer,
            new MiruHeaderRegion("soy.miru.chrome.headerRegion", renderer),
            new MiruAdminRegion("soy.miru.page.adminRegion", renderer),
            tenantRoutingProvider);
    }
}
