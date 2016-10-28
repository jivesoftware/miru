package com.jivesoftware.os.miru.tools.deployable.endpoints;

import com.jivesoftware.os.miru.tools.deployable.MiruToolsService;
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
@Path("/ui")
public class MiruToolsEndpoints {

    private final MiruToolsService toolsService;

    public MiruToolsEndpoints(@Context MiruToolsService toolsService) {
        this.toolsService = toolsService;
    }

    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response get() {
        String rendered = toolsService.render();
        return Response.ok(rendered).build();
    }

}
