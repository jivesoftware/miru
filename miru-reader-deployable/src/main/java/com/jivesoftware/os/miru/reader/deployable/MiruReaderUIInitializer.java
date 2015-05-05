package com.jivesoftware.os.miru.reader.deployable;

import com.jivesoftware.os.miru.api.MiruStats;

public class MiruReaderUIInitializer {

    public MiruReaderUIService initialize(MiruSoyRenderer renderer, MiruStats miruStats) throws Exception {

        return new MiruReaderUIService(
            renderer,
            new MiruHeaderRegion("soy.miru.chrome.headerRegion", renderer),
            new MiruAdminRegion("soy.miru.page.adminRegion", renderer, miruStats));
    }
}
