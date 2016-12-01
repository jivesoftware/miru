package com.jivesoftware.os.wiki.miru.deployable.endpoints;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.wiki.miru.deployable.WikiMiruService;
import com.jivesoftware.os.wiki.miru.deployable.region.WikiMiruIndexPluginRegion;
import com.jivesoftware.os.wiki.miru.deployable.region.WikiMiruIndexPluginRegion.WikiMiruIndexPluginRegionInput;
import javax.inject.Singleton;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang.StringUtils;

/**
 *
 */
@Singleton
@Path("/ui/index")
public class WikiMiruIndexPluginEndpoints {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final WikiMiruService wikiMiruService;
    private final WikiMiruIndexPluginRegion pluginRegion;

    public WikiMiruIndexPluginEndpoints(@Context WikiMiruService wikiMiruService, @Context WikiMiruIndexPluginRegion pluginRegion) {
        this.wikiMiruService = wikiMiruService;
        this.pluginRegion = pluginRegion;
    }

    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response index(
        @QueryParam("indexerId") @DefaultValue("") String indexerId,
        @QueryParam("tenantId") @DefaultValue("") String tenantId,
        @QueryParam("wikiDumpFile") @DefaultValue("") String wikiDumpFile,
        @QueryParam("batchSize") @DefaultValue("1000") int batchSize,
        @QueryParam("miruEnabled") @DefaultValue("true") boolean miruEnabled,
        @QueryParam("esClusterName") @DefaultValue("") String esClusterName,
        @QueryParam("esHosts") @DefaultValue("") String esHosts,
        @QueryParam("action") @DefaultValue("status") String action) {

        try {

            String rendered = wikiMiruService.renderPlugin(pluginRegion, new WikiMiruIndexPluginRegionInput(indexerId,
                tenantId,
                wikiDumpFile,
                batchSize,
                miruEnabled,
                StringUtils.trimToNull(esClusterName),
                Lists.newArrayList(Splitter.on(",").omitEmptyStrings().trimResults().split(esHosts)),
                action)
            );
            return Response.ok(rendered).build();

        } catch (Exception x) {
            LOG.error("Failed to generating index ui.", x);
            return Response.serverError().build();
        }
    }
}
