package com.jivesoftware.os.miru.anomaly.deployable.endpoints;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.anomaly.deployable.MiruAnomalyService;
import com.jivesoftware.os.miru.anomaly.deployable.region.AnomalyStatusPluginRegion;
import com.jivesoftware.os.miru.anomaly.deployable.region.AnomalyStatusPluginRegion.AnomalyStatusPluginRegionInput;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
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
@Path("/ui/anomaly/status")
public class AnomalyStatusPluginEndpoints {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruAnomalyService anomalyService;
    private final AnomalyStatusPluginRegion pluginRegion;

    public AnomalyStatusPluginEndpoints(@Context MiruAnomalyService anomalyService, @Context AnomalyStatusPluginRegion pluginRegion) {
        this.anomalyService = anomalyService;
        this.pluginRegion = pluginRegion;
    }

    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response status() {
        try {
            String rendered = anomalyService.renderPlugin(pluginRegion,
                Optional.of(new AnomalyStatusPluginRegionInput("foo")));
            return Response.ok(rendered).build();
        } catch (Throwable t) {
            LOG.error("Failed status", t);
            return Response.serverError().build();
        }
    }
}
