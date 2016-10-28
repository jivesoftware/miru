package com.jivesoftware.os.miru.tools.deployable.endpoints;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.tools.deployable.MiruToolsService;
import com.jivesoftware.os.miru.tools.deployable.region.AnalyticsPluginRegion;
import com.jivesoftware.os.miru.tools.deployable.region.AnalyticsPluginRegion.AnalyticsPluginRegionInput;
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
@Path("/ui/tools/analytics")
public class AnalyticsPluginEndpoints {

    private final MiruToolsService toolsService;
    private final AnalyticsPluginRegion analyticsPluginRegion;

    public AnalyticsPluginEndpoints(@Context MiruToolsService toolsService, @Context AnalyticsPluginRegion analyticsPluginRegion) {
        this.toolsService = toolsService;
        this.analyticsPluginRegion = analyticsPluginRegion;
    }


    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response getAnalytics(@QueryParam("tenantId") @DefaultValue("") String tenantId,
        @QueryParam("fromTimeAgo") @DefaultValue("720") long fromTimeAgo,
        @QueryParam("fromTimeUnit") @DefaultValue("HOURS") String fromTimeUnit,
        @QueryParam("toTimeAgo") @DefaultValue("0") long toTimeAgo,
        @QueryParam("toTimeUnit") @DefaultValue("HOURS") String toTimeUnit,
        @QueryParam("buckets") @DefaultValue("30") int buckets,
        @QueryParam("field1") @DefaultValue("activityType") String field1,
        @QueryParam("terms1") @DefaultValue("0, 1, 11, 65") String terms1,
        @QueryParam("field2") @DefaultValue("") String field2,
        @QueryParam("terms2") @DefaultValue("") String terms2,
        @QueryParam("filters") @DefaultValue("") String filters,
        @QueryParam("logLevel") @DefaultValue("NONE") String logLevel) {
        String rendered = toolsService.renderPlugin(analyticsPluginRegion,
            Optional.of(new AnalyticsPluginRegionInput(
                tenantId,
                fromTimeAgo,
                fromTimeUnit,
                toTimeAgo,
                toTimeUnit,
                buckets,
                field1.trim(),
                terms1.trim(),
                field2.trim(),
                terms2.trim(),
                filters.trim(),
                logLevel)));
        return Response.ok(rendered).build();
    }
}
