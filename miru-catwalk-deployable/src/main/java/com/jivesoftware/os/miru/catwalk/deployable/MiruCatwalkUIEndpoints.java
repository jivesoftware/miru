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
public class MiruCatwalkUIEndpoints {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruCatwalkUIService miruCatwalkUIService;

    public MiruCatwalkUIEndpoints(@Context MiruCatwalkUIService miruCatwalkUIService) {
        this.miruCatwalkUIService = miruCatwalkUIService;
    }

    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response get() {
        String rendered = miruCatwalkUIService.render();
        return Response.ok(rendered).build();
    }

    @GET
    @Path("/something")
    @Produces(MediaType.TEXT_HTML)
    public Response getHosts() {
        String rendered = miruCatwalkUIService.renderSomething();
        return Response.ok(rendered).build();
    }

}
