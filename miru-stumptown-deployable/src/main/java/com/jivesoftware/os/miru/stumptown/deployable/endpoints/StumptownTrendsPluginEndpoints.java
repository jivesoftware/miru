package com.jivesoftware.os.miru.stumptown.deployable.endpoints;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.stumptown.deployable.MiruStumptownService;
import com.jivesoftware.os.miru.stumptown.deployable.region.StumptownTrendsPluginRegion;
import com.jivesoftware.os.miru.stumptown.deployable.region.StumptownTrendsPluginRegion.TrendingPluginRegionInput;
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
@Path("/ui/trends")
public class StumptownTrendsPluginEndpoints {

    private final MiruStumptownService miruStumptownService;
    private final StumptownTrendsPluginRegion trendingPluginRegion;

    public StumptownTrendsPluginEndpoints(@Context MiruStumptownService miruStumptownService, @Context StumptownTrendsPluginRegion trendingPluginRegion) {
        this.miruStumptownService = miruStumptownService;
        this.trendingPluginRegion = trendingPluginRegion;
    }

    @GET
    @Produces(MediaType.TEXT_HTML)
    public Response getTrends(@QueryParam("logLevels") @DefaultValue("ERROR") String logLevels,
        @QueryParam("fromAgo") @DefaultValue("8") int fromAgo,
        @QueryParam("toAgo") @DefaultValue("0") int toAgo,
        @QueryParam("fromTimeUnit") @DefaultValue("MINUTES") String fromTimeUnit,
        @QueryParam("toTimeUnit") @DefaultValue("MINUTES") String toTimeUnit,
        @QueryParam("buckets") @DefaultValue("30") int buckets,
        @QueryParam("service") @DefaultValue("") String service,
        @QueryParam("aggregateAroundField") @DefaultValue("service") String aggregateAroundField,
        @QueryParam("strategy") @DefaultValue("LINEAR_REGRESSION") String strategy
    ) {

        if (service.trim().isEmpty()) {
            service = null;
        }
        String rendered = miruStumptownService.renderPlugin(trendingPluginRegion,
            Optional.of(new TrendingPluginRegionInput(logLevels, fromAgo, toAgo, fromTimeUnit, toTimeUnit, buckets, service, aggregateAroundField, strategy)));
        return Response.ok(rendered).build();
    }
}
