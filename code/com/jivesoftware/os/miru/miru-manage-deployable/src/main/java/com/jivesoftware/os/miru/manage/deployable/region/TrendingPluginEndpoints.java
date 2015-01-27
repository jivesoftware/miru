package com.jivesoftware.os.miru.manage.deployable.region;

import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.manage.deployable.MiruManageService;
import com.jivesoftware.os.miru.manage.deployable.region.TrendingPluginRegion.TrendingPluginRegionInput;
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
@Path("/miru/manage/trending")
public class TrendingPluginEndpoints {

    private final MiruManageService miruManageService;
    private final TrendingPluginRegion trendingPluginRegion;

    public TrendingPluginEndpoints(@Context MiruManageService miruManageService, @Context TrendingPluginRegion trendingPluginRegion) {
        this.miruManageService = miruManageService;
        this.trendingPluginRegion = trendingPluginRegion;
    }

    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response getTenantsForTenant(@QueryParam("tenantId") @DefaultValue("") String tenantId,
        @QueryParam("fromHoursAgo") @DefaultValue("72") int fromHoursAgo,
        @QueryParam("toHoursAgo") @DefaultValue("0") int toHoursAgo,
        @QueryParam("buckets") @DefaultValue("30") int buckets,
        @QueryParam("field") @DefaultValue("authors") String field,
        @QueryParam("fieldPrefixes") @DefaultValue("") String fieldPrefixesString,
        @QueryParam("logLevel") @DefaultValue("NONE") String logLevel) {

        List<String> fieldPrefixes = null;
        if (fieldPrefixesString != null && !fieldPrefixesString.isEmpty()) {
            fieldPrefixes = Lists.newArrayList(Splitter.onPattern("\\s*,\\s*").split(fieldPrefixesString));
            if (fieldPrefixes.isEmpty()) {
                fieldPrefixes = null;
            }
        }
        String rendered = miruManageService.renderPlugin(trendingPluginRegion,
            Optional.of(new TrendingPluginRegionInput(
                tenantId,
                fromHoursAgo,
                toHoursAgo,
                buckets,
                field,
                fieldPrefixes,
                logLevel)));
        return Response.ok(rendered).build();
    }
}
