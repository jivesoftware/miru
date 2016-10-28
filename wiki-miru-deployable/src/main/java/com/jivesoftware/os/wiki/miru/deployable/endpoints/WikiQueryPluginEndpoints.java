package com.jivesoftware.os.wiki.miru.deployable.endpoints;

import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.wiki.miru.deployable.WikiMiruService;
import com.jivesoftware.os.wiki.miru.deployable.region.WikiQueryPluginRegion;
import com.jivesoftware.os.wiki.miru.deployable.region.WikiQueryPluginRegion.WikiMiruPluginRegionInput;
import javax.inject.Singleton;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 *
 */
@Singleton
@Path("/ui/query")
public class WikiQueryPluginEndpoints {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final WikiMiruService wikiMiruService;
    private final WikiQueryPluginRegion pluginRegion;

    public WikiQueryPluginEndpoints(@Context WikiMiruService wikiMiruService, @Context WikiQueryPluginRegion pluginRegion) {
        this.wikiMiruService = wikiMiruService;
        this.pluginRegion = pluginRegion;
    }

    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response query(
        @QueryParam("subject") @DefaultValue("") String subject,
        @QueryParam("body") @DefaultValue("") String body) {

        try {
            String rendered = wikiMiruService.renderPlugin(pluginRegion, new WikiMiruPluginRegionInput(subject, body));
            return Response.ok(rendered).build();
        } catch (Exception x) {
            LOG.error("Failed to generating query ui.", x);
            return Response.serverError().build();
        }
    }

}
