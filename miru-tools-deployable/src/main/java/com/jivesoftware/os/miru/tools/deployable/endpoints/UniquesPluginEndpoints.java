package com.jivesoftware.os.miru.tools.deployable.endpoints;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.tools.deployable.MiruToolsService;
import com.jivesoftware.os.miru.tools.deployable.region.UniquesPluginRegion;
import com.jivesoftware.os.miru.tools.deployable.region.UniquesPluginRegion.UniquesPluginRegionInput;

import javax.inject.Singleton;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Singleton
@Path("/ui/tools/uniques")
public class UniquesPluginEndpoints {

    private final MiruToolsService toolsService;
    private final UniquesPluginRegion uniquesPluginRegion;

    public UniquesPluginEndpoints(@Context MiruToolsService toolsService,
                                  @Context UniquesPluginRegion uniquesPluginRegion) {
        this.toolsService = toolsService;
        this.uniquesPluginRegion = uniquesPluginRegion;
    }

    @GET
    @Produces(MediaType.TEXT_HTML)
    public Response getDistincts(@QueryParam("tenantId") @DefaultValue("") String tenantId,
                                 @QueryParam("fromHoursAgo") @DefaultValue("720") int fromHoursAgo,
                                 @QueryParam("toHoursAgo") @DefaultValue("0") int toHoursAgo,
                                 @QueryParam("field") @DefaultValue("") String field,
                                 @QueryParam("types") @DefaultValue("") String types,
                                 @QueryParam("filters") @DefaultValue("") String filters,
                                 @QueryParam("logLevel") @DefaultValue("NONE") String logLevel) {
        return Response.ok(
                toolsService.renderPlugin(
                        uniquesPluginRegion,
                        Optional.of(new UniquesPluginRegionInput(
                                tenantId,
                                fromHoursAgo,
                                toHoursAgo,
                                field,
                                types,
                                filters,
                                logLevel)))).build();
    }

}
