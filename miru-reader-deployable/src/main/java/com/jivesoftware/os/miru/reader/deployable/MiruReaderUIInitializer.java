package com.jivesoftware.os.miru.reader.deployable;

import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.service.MiruService;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.routing.bird.shared.TenantRoutingProvider;

public class MiruReaderUIInitializer {

    public MiruReaderUIService initialize(MiruSoyRenderer renderer,
        MiruStats miruStats,
        MiruService service,
        TenantRoutingProvider tenantRoutingProvider) throws Exception {

        return new MiruReaderUIService(
            renderer,
            new MiruHeaderRegion("soy.miru.chrome.headerRegion", renderer),
            new MiruAdminRegion("soy.miru.page.adminRegion", renderer, miruStats),
            new MiruPartitionsRegion("soy.miru.page.partitionsRegion", renderer, service),
            tenantRoutingProvider);
    }
}
