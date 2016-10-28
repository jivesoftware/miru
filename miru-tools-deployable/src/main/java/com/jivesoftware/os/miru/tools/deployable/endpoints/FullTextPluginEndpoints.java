package com.jivesoftware.os.miru.tools.deployable.endpoints;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.stream.plugins.fulltext.FullTextQuery;
import com.jivesoftware.os.miru.tools.deployable.MiruToolsService;
import com.jivesoftware.os.miru.tools.deployable.region.FullTextPluginRegion;
import com.jivesoftware.os.miru.tools.deployable.region.FullTextPluginRegion.FullTextPluginRegionInput;
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
@Path("/ui/tools/fulltext")
public class FullTextPluginEndpoints {

    private final MiruToolsService toolsService;
    private final FullTextPluginRegion fullTextPluginRegion;

    public FullTextPluginEndpoints(@Context MiruToolsService toolsService, @Context FullTextPluginRegion fullTextPluginRegion) {
        this.toolsService = toolsService;
        this.fullTextPluginRegion = fullTextPluginRegion;
    }

    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response getFullText(@QueryParam("tenantId") @DefaultValue("") String tenantId,
        @QueryParam("fromHoursAgo") @DefaultValue("720") int fromHoursAgo,
        @QueryParam("toHoursAgo") @DefaultValue("0") int toHoursAgo,
        @QueryParam("defaultField") @DefaultValue("") String defaultField,
        @QueryParam("locale") @DefaultValue("en") String locale,
        @QueryParam("queryString") @DefaultValue("") String queryString,
        @QueryParam("strategy") @DefaultValue("TIME") String strategy,
        @QueryParam("filters") @DefaultValue("") String filters,
        @QueryParam("maxCount") @DefaultValue("1000") int maxCount,
        @QueryParam("logLevel") @DefaultValue("NONE") String logLevel) {

        String rendered = toolsService.renderPlugin(fullTextPluginRegion,
            Optional.of(new FullTextPluginRegionInput(
                tenantId,
                fromHoursAgo,
                toHoursAgo,
                defaultField,
                locale,
                queryString,
                FullTextQuery.Strategy.valueOf(strategy),
                filters,
                maxCount,
                logLevel)));
        return Response.ok(rendered).build();
    }
}
