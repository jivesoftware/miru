package com.jivesoftware.os.wiki.miru.deployable.endpoints;

import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.wiki.miru.deployable.WikiMiruService;
import com.jivesoftware.os.wiki.miru.deployable.region.WikiMiruStressPluginRegion;
import com.jivesoftware.os.wiki.miru.deployable.region.WikiMiruStressPluginRegion.WikiMiruStressPluginRegionInput;
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
@Path("/ui/stress")
public class WikiMiruStressPluginEndpoints {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final WikiMiruService wikiMiruService;
    private final WikiMiruStressPluginRegion pluginRegion;

    public WikiMiruStressPluginEndpoints(@Context WikiMiruService wikiMiruService, @Context WikiMiruStressPluginRegion pluginRegion) {
        this.wikiMiruService = wikiMiruService;
        this.pluginRegion = pluginRegion;
    }

    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response index(
        @QueryParam("stresserId") @DefaultValue("") String stresserId,
        @QueryParam("tenantId") @DefaultValue("") String tenantId,
        @QueryParam("concurrency") @DefaultValue("10") int concurrency,
        @QueryParam("qps") @DefaultValue("10") int qps,
        @QueryParam("queryPhrases") @DefaultValue("") String queryPhrases,
        @QueryParam("queryPhraseFile") @DefaultValue("") String queryPhraseFile,
        @QueryParam("numberOfResult") @DefaultValue("100") int numberOfResult,
        @QueryParam("wildcardExpansion") @DefaultValue("false") boolean wildcardExpansion,
        @QueryParam("querier") @DefaultValue("miru") String querier,
        @QueryParam("action") @DefaultValue("status") String action) {

        try {

            String rendered = wikiMiruService.renderPlugin(pluginRegion,
                new WikiMiruStressPluginRegionInput(stresserId, tenantId, concurrency, qps, querier, numberOfResult, wildcardExpansion, queryPhrases,
                    queryPhraseFile, action));
            return Response.ok(rendered).build();

        } catch (Exception x) {
            LOG.error("Failed to generating index ui.", x);
            return Response.serverError().build();
        }
    }
}
