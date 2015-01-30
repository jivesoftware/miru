package com.jivesoftware.os.miru.lumberyard.deployable;

import com.jivesoftware.os.miru.lumberyard.deployable.region.MiruHeaderRegion;
import com.jivesoftware.os.miru.lumberyard.deployable.region.MiruHomeRegion;

public class MiruQueryLumberyardInitializer {

    public MiruQueryLumberyardService initialize(MiruSoyRenderer renderer) throws Exception {

        return new MiruQueryLumberyardService(
            renderer,
            new MiruHeaderRegion("soy.miru.chrome.headerRegion", renderer),
            new MiruHomeRegion("soy.miru.page.homeRegion", renderer)
        );
    }

}
