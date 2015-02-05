package com.jivesoftware.os.miru.sea.anomaly.deployable;

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
@Path("/")
public class MiruQuerySeaAnomalyEndpoints {

    private final MiruSeaAnomalyService miruQuerySeaAnomalyService;

    public MiruQuerySeaAnomalyEndpoints(@Context MiruSeaAnomalyService seaAnomalyService) {
        this.miruQuerySeaAnomalyService = seaAnomalyService;
    }

    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response get() {
        String rendered = miruQuerySeaAnomalyService.render();
        return Response.ok(rendered).build();
    }

}
