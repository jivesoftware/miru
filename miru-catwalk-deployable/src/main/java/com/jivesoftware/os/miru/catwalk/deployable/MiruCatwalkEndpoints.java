package com.jivesoftware.os.miru.catwalk.deployable;

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
@Path("/miru/catwalk")
public class MiruCatwalkEndpoints {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruCatwalkService miruCatwalkService;

    public MiruCatwalkEndpoints(@Context MiruCatwalkService miruCatwalkService) {
        this.miruCatwalkService = miruCatwalkService;
    }

    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response get() {
        String rendered = miruCatwalkService.render();
        return Response.ok(rendered).build();
    }

    @GET
    @Path("/something")
    @Produces(MediaType.TEXT_HTML)
    public Response getHosts() {
        String rendered = miruCatwalkService.renderSomething();
        return Response.ok(rendered).build();
    }

}
