package com.jivesoftware.os.miru.sea.anomaly.deployable;

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
@Path("/")
public class MiruQuerySeaAnomalyEndpoints {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruSeaAnomalyService miruQuerySeaAnomalyService;

    public MiruQuerySeaAnomalyEndpoints(@Context MiruSeaAnomalyService seaAnomalyService) {
        this.miruQuerySeaAnomalyService = seaAnomalyService;
    }

    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response get(@Context UriInfo uriInfo) {
        try {
            String rendered = miruQuerySeaAnomalyService.render(uriInfo.getAbsolutePath() + "/miru/sea/anomaly/intake");
            return Response.ok(rendered).build();
        } catch (Throwable t) {
            LOG.error("Failed get", t);
            return Response.serverError().build();
        }
    }

}
