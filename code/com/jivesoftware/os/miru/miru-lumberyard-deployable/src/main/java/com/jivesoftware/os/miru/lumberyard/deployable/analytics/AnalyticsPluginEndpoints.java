package com.jivesoftware.os.miru.lumberyard.deployable.analytics;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.lumberyard.deployable.MiruQueryLumberyardService;
import com.jivesoftware.os.miru.lumberyard.deployable.region.AnalyticsPluginRegion;
import com.jivesoftware.os.miru.lumberyard.deployable.region.AnalyticsPluginRegion.AnalyticsPluginRegionInput;
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
@Path("/miru/lumberyard/analytics")
public class AnalyticsPluginEndpoints {

    private final MiruQueryLumberyardService lumberyardService;
    private final AnalyticsPluginRegion analyticsPluginRegion;

    public AnalyticsPluginEndpoints(@Context MiruQueryLumberyardService lumberyardService, @Context AnalyticsPluginRegion analyticsPluginRegion) {
        this.lumberyardService = lumberyardService;
        this.analyticsPluginRegion = analyticsPluginRegion;
    }


    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response getTenantsForTenant(@QueryParam("tenantId") @DefaultValue("") String tenantId,
        @QueryParam("fromHoursAgo") @DefaultValue("720") int fromHoursAgo,
        @QueryParam("toHoursAgo") @DefaultValue("0") int toHoursAgo,
        @QueryParam("buckets") @DefaultValue("30") int buckets,
        @QueryParam("activityTypes") @DefaultValue("0, 1, 11, 65") String activityTypes,
        @QueryParam("users") @DefaultValue("") String users,
        @QueryParam("logLevel") @DefaultValue("NONE") String logLevel) {
        String rendered = lumberyardService.renderPlugin(analyticsPluginRegion,
            Optional.of(new AnalyticsPluginRegionInput(
                tenantId,
                fromHoursAgo,
                toHoursAgo,
                buckets,
                activityTypes,
                users,
                logLevel)));
        return Response.ok(rendered).build();
    }
}
