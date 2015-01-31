package com.jivesoftware.os.miru.lumberyard.deployable.analytics;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.jivesoftware.os.miru.lumberyard.deployable.MiruQueryLumberyardService;
import com.jivesoftware.os.miru.lumberyard.deployable.region.LumberyardQueryPluginRegion;
import com.jivesoftware.os.miru.lumberyard.deployable.region.LumberyardQueryPluginRegion.LumberyardPluginRegionInput;
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
        @QueryParam("service") @DefaultValue("") String service,
        @QueryParam("instance") @DefaultValue("") String instance,
        @QueryParam("version") @DefaultValue("") String version,
        @QueryParam("logLevels") @DefaultValue("INFO") List<String> logLevels,
        @QueryParam("fromAgo") @DefaultValue("8") int fromAgo,
        @QueryParam("toAgo") @DefaultValue("0") int toAgo,
        @QueryParam("fromTimeUnit") @DefaultValue("MINUTES") String fromTimeUnit,
        @QueryParam("toTimeUnit") @DefaultValue("MINUTES") String toTimeUnit,
        @QueryParam("thread") @DefaultValue("") String thread,
        @QueryParam("logger") @DefaultValue("") String logger,
        @QueryParam("message") @DefaultValue("") String message,
        @QueryParam("thrown") @DefaultValue("") String thrown,
        @QueryParam("buckets") @DefaultValue("30") int buckets,
        @QueryParam("desiredNumberOfResultsPerWaveform") @DefaultValue("100") int messageCount) {
        String rendered = lumberyardService.renderPlugin(pluginRegion,
            Optional.of(new LumberyardPluginRegionInput(cluster,
                host,
                service,
                instance,
                version,
                Joiner.on(',').join(logLevels),
                fromAgo,
                toAgo,
                fromTimeUnit,
                toTimeUnit,
                thread,
                logger,
                message,
                thrown,
                buckets,
                messageCount)));
        return Response.ok(rendered).build();
    }
}
