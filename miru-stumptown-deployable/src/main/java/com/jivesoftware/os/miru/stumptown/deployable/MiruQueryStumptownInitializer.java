package com.jivesoftware.os.miru.stumptown.deployable;

import com.jivesoftware.os.miru.stumptown.deployable.region.MiruHeaderRegion;
import com.jivesoftware.os.miru.stumptown.deployable.region.MiruHomeRegion;

public class MiruQueryStumptownInitializer {

    public MiruStumptownService initialize(MiruSoyRenderer renderer) throws Exception {

        return new MiruStumptownService(
            renderer,
            new MiruHeaderRegion("soy.stumptown.chrome.headerRegion", renderer),
            new MiruHomeRegion("soy.stumptown.page.homeRegion", renderer)
        );
    }

}
