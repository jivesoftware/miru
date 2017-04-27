package com.jivesoftware.os.miru.siphon.deployable.endpoints;

import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.miru.siphon.deployable.WikiMiruService;
import com.jivesoftware.os.miru.siphon.deployable.region.WikiWikiPluginRegion;
import com.jivesoftware.os.miru.siphon.deployable.region.WikiWikiPluginRegion.WikiWikiPluginRegionInput;
import javax.inject.Singleton;
import javax.ws.rs.DefaultValue;
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
@Path("/ui/wiki")
public class WikiWikiPluginEndpoints {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final WikiMiruService wikiMiruService;
    private final WikiWikiPluginRegion pluginRegion;

    public WikiWikiPluginEndpoints(@Context WikiMiruService wikiMiruService, @Context WikiWikiPluginRegion pluginRegion) {
        this.wikiMiruService = wikiMiruService;
        this.pluginRegion = pluginRegion;
    }

    @GET
    @Path("/{tenantId}/{guid}")
    @Produces(MediaType.TEXT_HTML)
    public Response wiki(
        @PathParam("tenantId") @DefaultValue("") String tenantId,
        @PathParam("guid") @DefaultValue("") String guid) {

        try {
            String rendered = wikiMiruService.renderPlugin(pluginRegion, new WikiWikiPluginRegionInput(tenantId, guid));
            return Response.ok(rendered).build();
        } catch (Exception x) {
            LOG.error("Failed to generating query ui.", x);
            return Response.serverError().build();
        }
    }
}
