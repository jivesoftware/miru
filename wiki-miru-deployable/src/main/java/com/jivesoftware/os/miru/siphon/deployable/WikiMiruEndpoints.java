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
public class WikiMiruEndpoints {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final WikiMiruService wikiMiruService;

    public WikiMiruEndpoints(@Context WikiMiruService wikiMiruService) {
        this.wikiMiruService = wikiMiruService;
    }

    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response get(@Context UriInfo uriInfo) {
        try {
            String rendered = wikiMiruService.render();
            return Response.ok(rendered).build();
        } catch (Exception x) {
            LOG.error("Failed to generating ui.", x);
            return Response.serverError().build();
        }
    }

}
