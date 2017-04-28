package com.jivesoftware.os.miru.wal.deployable;

import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.api.wal.AmzaCursor;
import com.jivesoftware.os.miru.api.wal.AmzaSipCursor;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.miru.api.wal.RCVSCursor;
import com.jivesoftware.os.miru.api.wal.RCVSSipCursor;
import com.jivesoftware.os.miru.ui.MiruAdminRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.miru.wal.AmzaWALUtil;
import com.jivesoftware.os.miru.wal.MiruWALDirector;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReader;
import com.jivesoftware.os.miru.wal.deployable.region.MiruActivityWALRegion;
import com.jivesoftware.os.miru.wal.deployable.region.MiruCleanupRegion;
import com.jivesoftware.os.miru.wal.deployable.region.MiruHeaderRegion;
import com.jivesoftware.os.miru.wal.deployable.region.MiruReadWALRegion;
import com.jivesoftware.os.miru.wal.deployable.region.MiruRepairRegion;
import com.jivesoftware.os.routing.bird.shared.TenantRoutingProvider;

public class MiruWALUIServiceInitializer {

    public MiruWALUIService initialize(String cluster,
        int instance,
        MiruSoyRenderer renderer,
        AmzaWALUtil amzaWALUtil,
        TenantRoutingProvider tenantRoutingProvider,
        MiruWALClient<?, ?> miruWALClient,
        MiruWALDirector miruWALDirector,
        MiruWALClient<RCVSCursor, RCVSSipCursor> rcvsWALClient,
        MiruWALClient<AmzaCursor, AmzaSipCursor> amzaWALClient,
        MiruActivityWALReader activityWALReader,
        MiruStats miruStats)
        throws Exception {

        return new MiruWALUIService(
            renderer,
            new MiruHeaderRegion(cluster, instance, "soy.miru.chrome.headerRegion", renderer, tenantRoutingProvider),
            new MiruAdminRegion("soy.miru.page.adminRegion", renderer, miruStats),
            new MiruActivityWALRegion("soy.miru.page.activityWalRegion", renderer, amzaWALUtil, rcvsWALClient, amzaWALClient),
            new MiruReadWALRegion("soy.miru.page.readWalRegion", renderer, rcvsWALClient), //TODO amza
            new MiruRepairRegion("soy.miru.page.repairRegion", renderer, activityWALReader, miruWALClient),
            new MiruCleanupRegion("soy.miru.page.cleanupRegion", renderer, miruWALClient, miruWALDirector));
    }
}
