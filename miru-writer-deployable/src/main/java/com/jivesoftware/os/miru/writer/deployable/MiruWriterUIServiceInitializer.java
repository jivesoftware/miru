package com.jivesoftware.os.miru.writer.deployable;

import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.miru.writer.deployable.region.MiruAdminRegion;
import com.jivesoftware.os.miru.writer.deployable.region.MiruHeaderRegion;
import com.jivesoftware.os.routing.bird.shared.TenantRoutingProvider;

public class MiruWriterUIServiceInitializer {

    public MiruWriterUIService initialize(MiruSoyRenderer renderer,
        MiruStats miruStats,
        TenantRoutingProvider tenantRoutingProvider)
        throws Exception {

        return new MiruWriterUIService(
            renderer,
            new MiruHeaderRegion("soy.miru.chrome.headerRegion", renderer, tenantRoutingProvider),
            new MiruAdminRegion("soy.miru.page.adminRegion", renderer, miruStats));
    }
}
