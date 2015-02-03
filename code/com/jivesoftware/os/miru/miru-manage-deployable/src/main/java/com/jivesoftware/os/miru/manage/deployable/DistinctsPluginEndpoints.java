package com.jivesoftware.os.miru.manage.deployable;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.manage.deployable.region.DistinctsPluginRegion;
import com.jivesoftware.os.miru.manage.deployable.region.DistinctsPluginRegion.DistinctsPluginRegionInput;
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
@Path("/miru/manage/distincts")
public class DistinctsPluginEndpoints {

    private final MiruManageService miruManageService;
    private final DistinctsPluginRegion distinctsPluginRegion;

    public DistinctsPluginEndpoints(@Context MiruManageService miruManageService, @Context DistinctsPluginRegion distinctsPluginRegion) {
        this.miruManageService = miruManageService;
        this.distinctsPluginRegion = distinctsPluginRegion;
    }


    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response getTenantsForTenant(@QueryParam("tenantId") @DefaultValue("") String tenantId,
        @QueryParam("fromHoursAgo") @DefaultValue("720") int fromHoursAgo,
        @QueryParam("toHoursAgo") @DefaultValue("0") int toHoursAgo,
        @QueryParam("field") @DefaultValue("") String field,
        @QueryParam("types") @DefaultValue("") String types,
        @QueryParam("filters") @DefaultValue("") String filters,
        @QueryParam("maxCount") @DefaultValue("1000") int maxCount,
        @QueryParam("logLevel") @DefaultValue("NONE") String logLevel) {

        String rendered = miruManageService.renderPlugin(distinctsPluginRegion,
            Optional.of(new DistinctsPluginRegionInput(
                tenantId,
                fromHoursAgo,
                toHoursAgo,
                field,
                types,
                filters,
                maxCount,
                logLevel)));
        return Response.ok(rendered).build();
    }
}
