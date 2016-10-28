package com.jivesoftware.os.miru.tools.deployable.endpoints;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.tools.deployable.MiruToolsService;
import com.jivesoftware.os.miru.tools.deployable.region.AggregateCountsPluginRegion;
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
@Path("/ui/tools/aggregate")
public class AggregateCountsPluginEndpoints {

    private final MiruToolsService toolsService;
    private final AggregateCountsPluginRegion aggregateCountsPluginRegion;

    public AggregateCountsPluginEndpoints(@Context MiruToolsService toolsService, @Context AggregateCountsPluginRegion aggregateCountsPluginRegion) {
        this.toolsService = toolsService;
        this.aggregateCountsPluginRegion = aggregateCountsPluginRegion;
    }

    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response getAggregates(@QueryParam("tenantId") @DefaultValue("") String tenantId,
        @QueryParam("forUser") @DefaultValue("") String forUser,
        @QueryParam("inbox") @DefaultValue("false") String inbox,
        @QueryParam("fromTimestamp") @DefaultValue("-1") long fromTimestamp,
        @QueryParam("field") @DefaultValue("parent") String field,
        @QueryParam("streamFilters") @DefaultValue("") String streamFilters,
        @QueryParam("constraintsFilters") @DefaultValue("") String constraintsFilters,
        @QueryParam("count") @DefaultValue("10") String count,
        @QueryParam("pages") @DefaultValue("3") String pages,
        @QueryParam("logLevel") @DefaultValue("NONE") String logLevel) {
        String rendered = toolsService.renderPlugin(aggregateCountsPluginRegion,
            Optional.of(new AggregateCountsPluginRegion.AggregateCountsPluginRegionInput(
                tenantId,
                forUser.trim(),
                Boolean.parseBoolean(inbox),
                fromTimestamp,
                field.trim(),
                streamFilters.trim(),
                constraintsFilters.trim(),
                count.isEmpty() ? 10 : Integer.parseInt(count),
                pages.isEmpty() ? 3 : Integer.parseInt(pages),
                logLevel)));
        return Response.ok(rendered).build();
    }
}
