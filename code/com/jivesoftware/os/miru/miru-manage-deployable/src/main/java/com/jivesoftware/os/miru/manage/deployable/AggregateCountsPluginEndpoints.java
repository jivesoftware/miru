package com.jivesoftware.os.miru.manage.deployable;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.manage.deployable.region.AggregateCountsPluginRegion;
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
@Path("/miru/manage/aggregate")
public class AggregateCountsPluginEndpoints {

    private final MiruManageService miruManageService;
    private final AggregateCountsPluginRegion aggregateCountsPluginRegion;

    public AggregateCountsPluginEndpoints(@Context MiruManageService miruManageService, @Context AggregateCountsPluginRegion aggregateCountsPluginRegion) {
        this.miruManageService = miruManageService;
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
        String rendered = miruManageService.renderPlugin(aggregateCountsPluginRegion,
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
