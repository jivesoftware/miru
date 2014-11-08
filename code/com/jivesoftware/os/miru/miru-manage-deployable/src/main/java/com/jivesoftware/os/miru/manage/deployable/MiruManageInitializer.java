package com.jivesoftware.os.miru.manage.deployable;

import com.jivesoftware.os.miru.cluster.MiruActivityLookupTable;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.cluster.MiruRegistryStore;
import com.jivesoftware.os.miru.cluster.rcvs.MiruRCVSActivityLookupTable;
import com.jivesoftware.os.miru.manage.deployable.region.MiruActivityWALRegion;
import com.jivesoftware.os.miru.manage.deployable.region.MiruAdminRegion;
import com.jivesoftware.os.miru.manage.deployable.region.MiruBalancerRegion;
import com.jivesoftware.os.miru.manage.deployable.region.MiruHeaderRegion;
import com.jivesoftware.os.miru.manage.deployable.region.MiruHostEntryRegion;
import com.jivesoftware.os.miru.manage.deployable.region.MiruHostFocusRegion;
import com.jivesoftware.os.miru.manage.deployable.region.MiruHostsRegion;
import com.jivesoftware.os.miru.manage.deployable.region.MiruLookupRegion;
import com.jivesoftware.os.miru.manage.deployable.region.MiruReadWALRegion;
import com.jivesoftware.os.miru.manage.deployable.region.MiruTenantEntryRegion;
import com.jivesoftware.os.miru.manage.deployable.region.MiruTenantsRegion;
import com.jivesoftware.os.miru.wal.MiruWALInitializer.MiruWAL;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReader;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReaderImpl;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALReader;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALReaderImpl;

public class MiruManageInitializer {

    public MiruManageService initialize(MiruSoyRenderer renderer,
        MiruClusterRegistry clusterRegistry,
        MiruRegistryStore registryStore,
        MiruWAL miruWAL)
        throws Exception {

        MiruActivityWALReader activityWALReader = new MiruActivityWALReaderImpl(miruWAL.getActivityWAL(), miruWAL.getActivitySipWAL());
        MiruReadTrackingWALReader readTrackingWALReader = new MiruReadTrackingWALReaderImpl(miruWAL.getReadTrackingWAL(), miruWAL.getReadTrackingSipWAL());
        MiruActivityLookupTable activityLookupTable = new MiruRCVSActivityLookupTable(registryStore.getActivityLookupTable());

        return new MiruManageService(
            renderer,
            new MiruHeaderRegion("soy.miru.chrome.headerRegion", renderer),
            new MiruAdminRegion("soy.miru.page.adminRegion", renderer),
            new MiruHostsRegion("soy.miru.page.hostsRegion", renderer, clusterRegistry,
                new MiruHostEntryRegion("soy.miru.section.hostEntryRegion", renderer),
                new MiruHostFocusRegion("soy.miru.section.hostFocusRegion", renderer, clusterRegistry)),
            new MiruBalancerRegion("soy.miru.page.balancerRegion", renderer, clusterRegistry),
            new MiruTenantsRegion("soy.miru.page.tenantsRegion", renderer,
                new MiruTenantEntryRegion("soy.miru.section.tenantEntryRegion", renderer, clusterRegistry)),
            new MiruLookupRegion("soy.miru.page.lookupRegion", renderer, activityLookupTable),
            new MiruActivityWALRegion("soy.miru.page.activityWalRegion", renderer, clusterRegistry, activityWALReader),
            new MiruReadWALRegion("soy.miru.page.readWalRegion", renderer, readTrackingWALReader));
    }
}
