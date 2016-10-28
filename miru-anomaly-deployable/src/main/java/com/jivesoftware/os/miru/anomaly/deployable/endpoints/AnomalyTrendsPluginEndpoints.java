package com.jivesoftware.os.miru.anomaly.deployable.endpoints;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.anomaly.deployable.MiruAnomalyService;
import com.jivesoftware.os.miru.anomaly.deployable.region.AnomalyTrendsPluginRegion;
import com.jivesoftware.os.miru.anomaly.deployable.region.AnomalyTrendsPluginRegion.TrendingPluginRegionInput;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
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
@Path("/ui/anomaly/trends")
public class AnomalyTrendsPluginEndpoints {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruAnomalyService miruAnomalyService;
    private final AnomalyTrendsPluginRegion trendingPluginRegion;

    public AnomalyTrendsPluginEndpoints(@Context MiruAnomalyService miruAnomalyService, @Context AnomalyTrendsPluginRegion trendingPluginRegion) {
        this.miruAnomalyService = miruAnomalyService;
        this.trendingPluginRegion = trendingPluginRegion;
    }

    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response getTrends(@QueryParam("type") @DefaultValue("counter") String type,
        @QueryParam("service") @DefaultValue("") String service) {

        try {
            if (service.trim().isEmpty()) {
                service = null;
            }
            String rendered = miruAnomalyService.renderPlugin(trendingPluginRegion,
                Optional.of(new TrendingPluginRegionInput(type, service)));
            return Response.ok(rendered).build();
        } catch (Throwable t) {
            LOG.error("Failed trends", t);
            return Response.serverError().build();
        }
    }
}
