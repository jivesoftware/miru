package com.jivesoftware.os.miru.sea.anomaly.deployable;

import com.jivesoftware.os.miru.sea.anomaly.deployable.region.MiruHeaderRegion;
import com.jivesoftware.os.miru.sea.anomaly.deployable.region.MiruHomeRegion;

public class MiruQuerySeaAnomalyInitializer {

    public MiruSeaAnomalyService initialize(MiruSoyRenderer renderer) throws Exception {

        return new MiruSeaAnomalyService(
            renderer,
            new MiruHeaderRegion("soy.sea.anomaly.chrome.headerRegion", renderer),
            new MiruHomeRegion("soy.sea.anomaly.page.homeRegion", renderer)
        );
    }

}
