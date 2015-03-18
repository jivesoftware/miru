package com.jivesoftware.os.miru.manage.deployable;

import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.manage.deployable.region.MiruAdminRegion;
import com.jivesoftware.os.miru.manage.deployable.region.MiruBalancerRegion;
import com.jivesoftware.os.miru.manage.deployable.region.MiruHeaderRegion;
import com.jivesoftware.os.miru.manage.deployable.region.MiruHostEntryRegion;
import com.jivesoftware.os.miru.manage.deployable.region.MiruHostFocusRegion;
import com.jivesoftware.os.miru.manage.deployable.region.MiruHostsRegion;
import com.jivesoftware.os.miru.manage.deployable.region.MiruSchemaRegion;
import com.jivesoftware.os.miru.manage.deployable.region.MiruTenantEntryRegion;
import com.jivesoftware.os.miru.manage.deployable.region.MiruTenantsRegion;

public class MiruManageInitializer {

    public MiruManageService initialize(MiruSoyRenderer renderer,
        MiruClusterRegistry clusterRegistry,
        MiruWALClient miruWALClient)
        throws Exception {

        return new MiruManageService(
            renderer,
            new MiruHeaderRegion("soy.miru.chrome.headerRegion", renderer),
            new MiruAdminRegion("soy.miru.page.adminRegion", renderer),
            new MiruHostsRegion("soy.miru.page.hostsRegion", renderer, clusterRegistry,
                new MiruHostEntryRegion("soy.miru.section.hostEntryRegion", renderer),
                new MiruHostFocusRegion("soy.miru.section.hostFocusRegion", renderer, clusterRegistry)),
            new MiruBalancerRegion("soy.miru.page.balancerRegion", renderer, clusterRegistry, miruWALClient),
            new MiruSchemaRegion("soy.miru.page.schemaRegion", renderer, clusterRegistry, miruWALClient),
            new MiruTenantsRegion("soy.miru.page.tenantsRegion", renderer,
                new MiruTenantEntryRegion("soy.miru.section.tenantEntryRegion", renderer, clusterRegistry, miruWALClient)));
    }
}
