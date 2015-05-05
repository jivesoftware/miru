package com.jivesoftware.os.miru.sea.anomaly.deployable.endpoints;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.sea.anomaly.deployable.MiruSeaAnomalyService;
import com.jivesoftware.os.miru.sea.anomaly.deployable.region.SeaAnomalyStatusPluginRegion;
import com.jivesoftware.os.miru.sea.anomaly.deployable.region.SeaAnomalyStatusPluginRegion.SeaAnomalyStatusPluginRegionInput;
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
@Path("/seaAnomaly/status")
public class SeaAnomalyStatusPluginEndpoints {

    private final MiruSeaAnomalyService seaAnomalyService;
    private final SeaAnomalyStatusPluginRegion pluginRegion;

    public SeaAnomalyStatusPluginEndpoints(@Context MiruSeaAnomalyService seaAnomalyService, @Context SeaAnomalyStatusPluginRegion pluginRegion) {
        this.seaAnomalyService = seaAnomalyService;
        this.pluginRegion = pluginRegion;
    }

    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response query() {
        String rendered = seaAnomalyService.renderPlugin(pluginRegion,
            Optional.of(new SeaAnomalyStatusPluginRegionInput("foo")));
        return Response.ok(rendered).build();
    }
}
