package com.jivesoftware.os.miru.stumptown.deployable.endpoints;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.stumptown.deployable.MiruStumptownService;
import com.jivesoftware.os.miru.stumptown.deployable.region.StumptownStatusPluginRegion;
import com.jivesoftware.os.miru.stumptown.deployable.region.StumptownStatusPluginRegion.StumptownStatusPluginRegionInput;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 *
 */
@Singleton
@Path("/ui/status")
public class StumptownStatusPluginEndpoints {

    private final MiruStumptownService stumptownService;
    private final StumptownStatusPluginRegion pluginRegion;

    public StumptownStatusPluginEndpoints(@Context MiruStumptownService stumptownService, @Context StumptownStatusPluginRegion pluginRegion) {
        this.stumptownService = stumptownService;
        this.pluginRegion = pluginRegion;
    }

    @GET
    @Produces(MediaType.TEXT_HTML)
    public Response query() {
        String rendered = stumptownService.renderPlugin(pluginRegion,
            Optional.of(new StumptownStatusPluginRegionInput("foo")));
        return Response.ok(rendered).build();
    }
}
