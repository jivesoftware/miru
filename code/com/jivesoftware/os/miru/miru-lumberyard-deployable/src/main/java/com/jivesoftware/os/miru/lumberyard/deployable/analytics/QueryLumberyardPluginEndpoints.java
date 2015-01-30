package com.jivesoftware.os.miru.lumberyard.deployable.analytics;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.lumberyard.deployable.MiruQueryLumberyardService;
import com.jivesoftware.os.miru.lumberyard.deployable.region.LumberyardQueryPluginRegion;
import com.jivesoftware.os.miru.lumberyard.deployable.region.LumberyardQueryPluginRegion.LumberyardPluginRegionInput;
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
@Path("/lumberyard/query")
public class QueryLumberyardPluginEndpoints {

    private final MiruQueryLumberyardService lumberyardService;
    private final LumberyardQueryPluginRegion pluginRegion;

    public QueryLumberyardPluginEndpoints(@Context MiruQueryLumberyardService lumberyardService, @Context LumberyardQueryPluginRegion pluginRegion) {
        this.lumberyardService = lumberyardService;
        this.pluginRegion = pluginRegion;
    }

    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response query(
        @QueryParam("cluster") @DefaultValue("dev") String cluster,
        @QueryParam("host") @DefaultValue("") String host,
        @QueryParam("version") @DefaultValue("") String version,
        @QueryParam("service") @DefaultValue("") String service,
        @QueryParam("instance") @DefaultValue("") String instance,
        @QueryParam("logLevel") @DefaultValue("INFO") String logLevel,
        @QueryParam("fromAgo") @DefaultValue("8") int fromAgo,
        @QueryParam("toAgo") @DefaultValue("0") int toAgo,
        @QueryParam("timeUnit") @DefaultValue("MINUTES") String timeUnit,
        @QueryParam("thread") @DefaultValue("") String thread,
        @QueryParam("logger") @DefaultValue("") String logger,
        @QueryParam("message") @DefaultValue("") String message,
        @QueryParam("tenantId") @DefaultValue("") String tenantId,
        @QueryParam("buckets") @DefaultValue("30") int buckets,
        @QueryParam("messageCount") @DefaultValue("100") int messageCount,
        @QueryParam("activityTypes") @DefaultValue("0, 1, 11, 65") String activityTypes,
        @QueryParam("users") @DefaultValue("") String users) {
        String rendered = lumberyardService.renderPlugin(pluginRegion,
            Optional.of(new LumberyardPluginRegionInput(cluster, host, version, service, instance, logLevel, fromAgo, toAgo, timeUnit, thread, logger, message,
                    tenantId, buckets, messageCount, activityTypes, users)));
        return Response.ok(rendered).build();
    }
}
