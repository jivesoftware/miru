package com.jivesoftware.os.miru.lumberyard.deployable.endpoints;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.lumberyard.deployable.MiruQueryLumberyardService;
import com.jivesoftware.os.miru.lumberyard.deployable.region.LumberyardStatusPluginRegion;
import com.jivesoftware.os.miru.lumberyard.deployable.region.LumberyardStatusPluginRegion.LumberyardStatusPluginRegionInput;
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
@Path("/lumberyard/status")
public class StatusLumberyardPluginEndpoints {

    private final MiruQueryLumberyardService lumberyardService;
    private final LumberyardStatusPluginRegion pluginRegion;

    public StatusLumberyardPluginEndpoints(@Context MiruQueryLumberyardService lumberyardService, @Context LumberyardStatusPluginRegion pluginRegion) {
        this.lumberyardService = lumberyardService;
        this.pluginRegion = pluginRegion;
    }

    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response query() {
        String rendered = lumberyardService.renderPlugin(pluginRegion,
            Optional.of(new LumberyardStatusPluginRegionInput("foo")));
        return Response.ok(rendered).build();
    }
}
