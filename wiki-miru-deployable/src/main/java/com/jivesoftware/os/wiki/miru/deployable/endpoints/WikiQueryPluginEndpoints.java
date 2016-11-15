package com.jivesoftware.os.wiki.miru.deployable.endpoints;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.wiki.miru.deployable.WikiMiruService;
import com.jivesoftware.os.wiki.miru.deployable.region.WikiQueryPluginRegion;
import com.jivesoftware.os.wiki.miru.deployable.region.WikiQueryPluginRegion.WikiMiruPluginRegionInput;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.inject.Singleton;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
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
        @QueryParam("tenantId") @DefaultValue("") String tenantId,
        @QueryParam("query") @DefaultValue("") String query) {

        try {
            String rendered = wikiMiruService.renderPlugin(pluginRegion, new WikiMiruPluginRegionInput(tenantId, query));
            return Response.ok(rendered).build();
        } catch (Exception x) {
            LOG.error("Failed to generating query ui.", x);
            return Response.serverError().build();
        }
    }

    @GET
    @Path("/typeahead/{tenantId}/{query}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response typeahead(
        @PathParam("tenantId") @DefaultValue("") String tenantId,
        @PathParam("query") @DefaultValue("") String query) {

        try {
            LOG.info("typeahead" + query);
            List<Map<String,String>> data = new ArrayList<>();
            data.add(ImmutableMap.of("key","1", "name", "foo"));
            data.add(ImmutableMap.of("key","2", "name", "bar"));
            data.add(ImmutableMap.of("key","3", "name", "bazz"));

            return Response.ok(new ObjectMapper().writeValueAsString(data)).build();
        } catch (Exception x) {
            LOG.error("Failed to generating query ui.", x);
            return Response.serverError().build();
        }
    }

}
