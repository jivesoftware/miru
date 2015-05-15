package com.jivesoftware.os.miru.writer.deployable;

import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.miru.writer.deployable.region.MiruAdminRegion;
import com.jivesoftware.os.miru.writer.deployable.region.MiruHeaderRegion;

public class MiruWriterUIServiceInitializer {

    public MiruWriterUIService initialize(MiruSoyRenderer renderer,
        MiruStats miruStats)
        throws Exception {

        return new MiruWriterUIService(
            renderer,
            new MiruHeaderRegion("soy.miru.chrome.headerRegion", renderer),
            new MiruAdminRegion("soy.miru.page.adminRegion", renderer, miruStats));
    }
}
