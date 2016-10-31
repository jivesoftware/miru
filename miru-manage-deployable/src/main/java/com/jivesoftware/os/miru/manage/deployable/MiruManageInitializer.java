package com.jivesoftware.os.miru.manage.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.manage.deployable.region.MiruBalancerRegion;
import com.jivesoftware.os.miru.manage.deployable.region.MiruHeaderRegion;
import com.jivesoftware.os.miru.manage.deployable.region.MiruHostEntryRegion;
import com.jivesoftware.os.miru.manage.deployable.region.MiruHostFocusRegion;
import com.jivesoftware.os.miru.manage.deployable.region.MiruHostsRegion;
import com.jivesoftware.os.miru.manage.deployable.region.MiruSchemaRegion;
import com.jivesoftware.os.miru.manage.deployable.region.MiruTenantEntryRegion;
import com.jivesoftware.os.miru.manage.deployable.region.MiruTenantsRegion;
import com.jivesoftware.os.miru.ui.MiruAdminRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.routing.bird.shared.TenantRoutingProvider;

public class MiruManageInitializer {

    public MiruManageService initialize(String cluster,
        int instance,
        MiruSoyRenderer renderer,
        MiruClusterRegistry clusterRegistry,
        MiruWALClient miruWALClient,
        MiruStats stats,
        TenantRoutingProvider tenantRoutingProvider,
        ObjectMapper mapper)
        throws Exception {

        return new MiruManageService(
            renderer,
            new MiruHeaderRegion(cluster, instance, "soy.miru.chrome.headerRegion", renderer, tenantRoutingProvider),
            new MiruAdminRegion("soy.miru.page.adminRegion", renderer, stats),
            new MiruHostsRegion("soy.miru.page.hostsRegion", renderer, clusterRegistry,
                new MiruHostEntryRegion("soy.miru.section.hostEntryRegion", renderer),
                new MiruHostFocusRegion("soy.miru.section.hostFocusRegion", renderer, clusterRegistry)),
            new MiruBalancerRegion("soy.miru.page.balancerRegion", renderer, clusterRegistry, miruWALClient),
            new MiruSchemaRegion("soy.miru.page.schemaRegion", renderer, clusterRegistry, miruWALClient, mapper),
            new MiruTenantsRegion("soy.miru.page.tenantsRegion", renderer,
                new MiruTenantEntryRegion("soy.miru.section.tenantEntryRegion", renderer, clusterRegistry, miruWALClient)));
    }
}
