package com.jivesoftware.os.miru.tools.deployable;

import com.jivesoftware.os.miru.tools.deployable.region.MiruHeaderRegion;

public class MiruToolsInitializer {

    public MiruToolsService initialize(MiruSoyRenderer renderer) throws Exception {

        return new MiruToolsService(
            renderer,
            new MiruHeaderRegion("soy.miru.chrome.headerRegion", renderer),
            new MiruAdminRegion("soy.miru.page.adminRegion", renderer));
    }
}
