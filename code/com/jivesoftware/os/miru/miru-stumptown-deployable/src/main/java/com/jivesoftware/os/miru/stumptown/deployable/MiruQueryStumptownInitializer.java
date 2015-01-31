package com.jivesoftware.os.miru.stumptown.deployable;

import com.jivesoftware.os.miru.stumptown.deployable.region.MiruHeaderRegion;
import com.jivesoftware.os.miru.stumptown.deployable.region.MiruHomeRegion;

public class MiruQueryStumptownInitializer {

    public MiruQueryStumptownService initialize(MiruSoyRenderer renderer) throws Exception {

        return new MiruQueryStumptownService(
            renderer,
            new MiruHeaderRegion("soy.miru.chrome.headerRegion", renderer),
            new MiruHomeRegion("soy.miru.page.homeRegion", renderer)
        );
    }

}
