package com.jivesoftware.os.wiki.miru.deployable;

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
public class WikiMiruQueryEndpoints {

    private final WikiMiruService wikiMiruService;

    public WikiMiruQueryEndpoints(@Context WikiMiruService wikiMiruService) {
        this.wikiMiruService = wikiMiruService;
    }

    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response get(@Context UriInfo uriInfo) {
        String rendered = wikiMiruService.render(uriInfo.getAbsolutePath() + "miru/wiki/intake");
        return Response.ok(rendered).build();
    }

}
