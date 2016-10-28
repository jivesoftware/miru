package com.jivesoftware.os.miru.tools.deployable.endpoints;

import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.tools.deployable.MiruToolsService;
import com.jivesoftware.os.miru.tools.deployable.region.TrendingPluginRegion;
import com.jivesoftware.os.miru.tools.deployable.region.TrendingPluginRegion.TrendingPluginRegionInput;
import java.util.List;
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
@Path("/ui/tools/trending")
public class TrendingPluginEndpoints {

    private final MiruToolsService toolsService;
    private final TrendingPluginRegion trendingPluginRegion;

    public TrendingPluginEndpoints(@Context MiruToolsService toolsService, @Context TrendingPluginRegion trendingPluginRegion) {
        this.toolsService = toolsService;
        this.trendingPluginRegion = trendingPluginRegion;
    }

    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response getTrending(@QueryParam("tenantId") @DefaultValue("") String tenantId,
        @QueryParam("fromHoursAgo") @DefaultValue("72") int fromHoursAgo,
        @QueryParam("toHoursAgo") @DefaultValue("0") int toHoursAgo,
        @QueryParam("buckets") @DefaultValue("30") int buckets,
        @QueryParam("field") @DefaultValue("authors") String field,
        @QueryParam("filter") @DefaultValue("") String filter,
        @QueryParam("subFilters") @DefaultValue("") String subFilters,
        @QueryParam("fieldPrefixes") @DefaultValue("") String fieldPrefixesString,
        @QueryParam("distinctsFilter") @DefaultValue("") String distinctsFilter,
        @QueryParam("logLevel") @DefaultValue("NONE") String logLevel) {

        List<String> fieldPrefixes = null;
        if (fieldPrefixesString != null && !fieldPrefixesString.isEmpty()) {
            fieldPrefixes = Lists.newArrayList(Splitter.onPattern("\\s*,\\s*").split(fieldPrefixesString));
            if (fieldPrefixes.isEmpty()) {
                fieldPrefixes = null;
            }
        }
        String rendered = toolsService.renderPlugin(trendingPluginRegion,
            Optional.of(new TrendingPluginRegionInput(
                tenantId.trim(),
                fromHoursAgo,
                toHoursAgo,
                buckets,
                field.trim(),
                filter.trim(),
                subFilters.trim(),
                fieldPrefixes,
                distinctsFilter.trim(),
                logLevel)));
        return Response.ok(rendered).build();
    }
}
