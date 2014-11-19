package com.jivesoftware.os.miru.manage.deployable;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.manage.deployable.region.AnalyticsPluginRegion;
import com.jivesoftware.os.miru.manage.deployable.region.AnalyticsPluginRegion.AnalyticsPluginRegionInput;
import javax.inject.Singleton;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 *
 */
@Singleton
@Path("/miru/manage/analytics")
public class AnalyticsPluginEndpoints {

    private final MiruManageService miruManageService;
    private final AnalyticsPluginRegion analyticsPluginRegion;

    public AnalyticsPluginEndpoints(@Context MiruManageService miruManageService, @Context AnalyticsPluginRegion analyticsPluginRegion) {
        this.miruManageService = miruManageService;
        this.analyticsPluginRegion = analyticsPluginRegion;
    }


    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response getTenantsForTenant(@QueryParam("tenantId") @DefaultValue("") String tenantId,
        @QueryParam("hours") @DefaultValue("720") int hours,
        @QueryParam("buckets") @DefaultValue("30") int buckets) {
        String rendered = miruManageService.renderPlugin(analyticsPluginRegion,
            Optional.of(new AnalyticsPluginRegionInput(
                    new MiruTenantId(tenantId.getBytes(Charsets.UTF_8)),
                    hours,
                    buckets)));
        return Response.ok(rendered).build();
    }
}
