package com.jivesoftware.os.miru.lumberyard.deployable;

import com.jivesoftware.os.miru.lumberyard.deployable.region.MiruAdminRegion;
import com.jivesoftware.os.miru.lumberyard.deployable.region.MiruHeaderRegion;

public class MiruQueryLumberyardInitializer {

    public MiruQueryLumberyardService initialize(MiruSoyRenderer renderer) throws Exception {

        return new MiruQueryLumberyardService(
            renderer,
            new MiruHeaderRegion("soy.miru.chrome.headerRegion", renderer),
            new MiruAdminRegion("soy.miru.page.adminRegion", renderer)
        );
    }

}
