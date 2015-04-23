package com.jivesoftware.os.miru.writer.deployable;

import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.wal.MiruWALDirector;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReader;
import com.jivesoftware.os.miru.writer.deployable.region.MiruActivityWALRegion;
import com.jivesoftware.os.miru.writer.deployable.region.MiruAdminRegion;
import com.jivesoftware.os.miru.writer.deployable.region.MiruHeaderRegion;
import com.jivesoftware.os.miru.writer.deployable.region.MiruLookupRegion;
import com.jivesoftware.os.miru.writer.deployable.region.MiruReadWALRegion;
import com.jivesoftware.os.miru.writer.deployable.region.MiruRepairRegion;

public class MiruWriterUIServiceInitializer {

    public MiruWriterUIService initialize(MiruSoyRenderer renderer,
        MiruWALDirector miruWALDirector,
        MiruActivityWALReader activityWALReader,
        MiruStats miruStats)
        throws Exception {

        return new MiruWriterUIService(
            renderer,
            new MiruHeaderRegion("soy.miru.chrome.headerRegion", renderer),
            new MiruAdminRegion("soy.miru.page.adminRegion", renderer, miruStats),
            new MiruLookupRegion("soy.miru.page.lookupRegion", renderer, miruWALDirector),
            new MiruActivityWALRegion("soy.miru.page.activityWalRegion", renderer, miruWALDirector),
            new MiruReadWALRegion("soy.miru.page.readWalRegion", renderer, miruWALDirector),
            new MiruRepairRegion("soy.miru.page.repairRegion", renderer, activityWALReader, miruWALDirector));
    }
}
