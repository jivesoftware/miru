package com.jivesoftware.os.miru.sea.anomaly.deployable.endpoints;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.sea.anomaly.deployable.MiruSeaAnomalyService;
import com.jivesoftware.os.miru.sea.anomaly.deployable.region.SeaAnomalyTrendsPluginRegion;
import com.jivesoftware.os.miru.sea.anomaly.deployable.region.SeaAnomalyTrendsPluginRegion.TrendingPluginRegionInput;
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
@Path("/seaAnomaly/trends")
public class SeaAnomalyTrendsPluginEndpoints {

    private final MiruSeaAnomalyService miruSeaAnomalyService;
    private final SeaAnomalyTrendsPluginRegion trendingPluginRegion;

    public SeaAnomalyTrendsPluginEndpoints(@Context MiruSeaAnomalyService miruSeaAnomalyService, @Context SeaAnomalyTrendsPluginRegion trendingPluginRegion) {
        this.miruSeaAnomalyService = miruSeaAnomalyService;
        this.trendingPluginRegion = trendingPluginRegion;
    }

    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response getTrends(@QueryParam("type") @DefaultValue("counter") String type,
        @QueryParam("service") @DefaultValue("") String service) {

        if (service.trim().isEmpty()) {
            service = null;
        }
        String rendered = miruSeaAnomalyService.renderPlugin(trendingPluginRegion,
            Optional.of(new TrendingPluginRegionInput(type, service)));
        return Response.ok(rendered).build();
    }
}
