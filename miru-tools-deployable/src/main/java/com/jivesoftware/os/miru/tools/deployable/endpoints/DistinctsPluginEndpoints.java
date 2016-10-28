package com.jivesoftware.os.miru.tools.deployable.endpoints;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.tools.deployable.MiruToolsService;
import com.jivesoftware.os.miru.tools.deployable.region.DistinctsPluginRegion;
import com.jivesoftware.os.miru.tools.deployable.region.DistinctsPluginRegion.DistinctsPluginRegionInput;
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
@Path("/ui/tools/distincts")
public class DistinctsPluginEndpoints {

    private final MiruToolsService toolsService;
    private final DistinctsPluginRegion distinctsPluginRegion;

    public DistinctsPluginEndpoints(@Context MiruToolsService toolsService, @Context DistinctsPluginRegion distinctsPluginRegion) {
        this.toolsService = toolsService;
        this.distinctsPluginRegion = distinctsPluginRegion;
    }


    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response getDistincts(@QueryParam("tenantId") @DefaultValue("") String tenantId,
        @QueryParam("fromHoursAgo") @DefaultValue("720") int fromHoursAgo,
        @QueryParam("toHoursAgo") @DefaultValue("0") int toHoursAgo,
        @QueryParam("field") @DefaultValue("") String field,
        @QueryParam("types") @DefaultValue("") String types,
        @QueryParam("filters") @DefaultValue("") String filters,
        @QueryParam("maxCount") @DefaultValue("1000") int maxCount,
        @QueryParam("logLevel") @DefaultValue("NONE") String logLevel) {

        String rendered = toolsService.renderPlugin(distinctsPluginRegion,
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
