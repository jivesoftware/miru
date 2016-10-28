package com.jivesoftware.os.miru.anomaly.deployable;

import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

/**
 *
 */
@Singleton
@Path("/ui")
public class MiruQueryAnomalyEndpoints {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruAnomalyService miruAnomalyService;

    public MiruQueryAnomalyEndpoints(@Context MiruAnomalyService anomalyService) {
        this.miruAnomalyService = anomalyService;
    }

    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response get(@Context UriInfo uriInfo) {
        try {
            String rendered = miruAnomalyService.render(uriInfo.getAbsolutePath() + "/miru/anomaly/intake");
            return Response.ok(rendered).build();
        } catch (Throwable t) {
            LOG.error("Failed get", t);
            return Response.serverError().build();
        }
    }

}
