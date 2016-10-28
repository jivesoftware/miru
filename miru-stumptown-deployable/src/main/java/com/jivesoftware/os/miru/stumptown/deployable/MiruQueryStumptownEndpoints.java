package com.jivesoftware.os.miru.stumptown.deployable;

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
public class MiruQueryStumptownEndpoints {

    private final MiruStumptownService miruQueryStumptownService;

    public MiruQueryStumptownEndpoints(@Context MiruStumptownService stumptownService) {
        this.miruQueryStumptownService = stumptownService;
    }

    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response get(@Context UriInfo uriInfo) {
        String rendered = miruQueryStumptownService.render("/miru/stumptown/intake");
        return Response.ok(rendered).build();
    }

}
