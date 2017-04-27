package com.jivesoftware.os.miru.siphon.deployable;

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
public class MiruSiphonEndpoints {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruSiphonService miruSiphonService;

    public MiruSiphonEndpoints(@Context MiruSiphonService miruSiphonService) {
        this.miruSiphonService = miruSiphonService;
    }

    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response get(@Context UriInfo uriInfo) {
        try {
            String rendered = miruSiphonService.render();
            return Response.ok(rendered).build();
        } catch (Exception x) {
            LOG.error("Failed to generating ui.", x);
            return Response.serverError().build();
        }
    }

}
