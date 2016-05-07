package com.jivesoftware.os.miru.anomaly.deployable;

import com.jivesoftware.os.miru.anomaly.deployable.region.MiruHeaderRegion;
import com.jivesoftware.os.miru.anomaly.deployable.region.MiruHomeRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;

public class MiruQueryAnomalyInitializer {

    public MiruAnomalyService initialize(MiruSoyRenderer renderer) throws Exception {

        return new MiruAnomalyService(
            renderer,
            new MiruHeaderRegion("soy.anomaly.chrome.headerRegion", renderer),
            new MiruHomeRegion("soy.anomaly.page.homeRegion", renderer)
        );
    }

}
