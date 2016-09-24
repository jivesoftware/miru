package com.jivesoftware.os.miru.reader.deployable;

import com.google.common.base.Optional;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 *
 */
@Singleton
@Path("/")
public class MiruReaderUIEndpoints {

    private final MiruReaderUIService service;

    public MiruReaderUIEndpoints(@Context MiruReaderUIService service) {
        this.service = service;
    }

    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response get() {
        String rendered = service.render();
        return Response.ok(rendered).build();
    }

    @GET
    @Path("/partitions")
    @Produces(MediaType.TEXT_HTML)
    public Response getPartitions() {
        String rendered = service.renderPartitions(Optional.<String>absent());
        return Response.ok(rendered).build();
    }

    @GET
    @Path("/partitions/{tenantId}")
    @Produces(MediaType.TEXT_HTML)
    public Response getPartitions(@PathParam("tenantId") String tenantId) {
        String rendered = service.renderPartitions(Optional.of(tenantId));
        return Response.ok(rendered).build();
    }

    @GET
    @Path("/errors")
    @Produces(MediaType.TEXT_HTML)
    public Response getErrors() {
        String rendered = service.renderErrors();
        return Response.ok(rendered).build();
    }

    @GET
    @Path("/labStats/{group}/{filter}")
    @Produces(MediaType.TEXT_HTML)
    public Response getLABStats(@PathParam("group") String group, @PathParam("filter") String filter) {
        String rendered = service.renderLabStats(group, filter);
        return Response.ok(rendered).build();
    }

}
