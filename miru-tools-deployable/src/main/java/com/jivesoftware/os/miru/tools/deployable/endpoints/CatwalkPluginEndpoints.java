package com.jivesoftware.os.miru.tools.deployable.endpoints;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.tools.deployable.MiruToolsService;
import com.jivesoftware.os.miru.tools.deployable.region.CatwalkPluginRegion;
import com.jivesoftware.os.miru.tools.deployable.region.CatwalkPluginRegion.CatwalkPluginRegionInput;
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
@Path("/miru/tools/catwalk")
public class CatwalkPluginEndpoints {

    private final MiruToolsService toolsService;
    private final CatwalkPluginRegion catwalkPluginRegion;

    public CatwalkPluginEndpoints(@Context MiruToolsService toolsService, @Context CatwalkPluginRegion catwalkPluginRegion) {
        this.toolsService = toolsService;
        this.catwalkPluginRegion = catwalkPluginRegion;
    }


    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response getAnalytics(@QueryParam("tenantId") @DefaultValue("") String tenantId,
        @QueryParam("fromTimeAgo") @DefaultValue("720") long fromTimeAgo,
        @QueryParam("fromTimeUnit") @DefaultValue("HOURS") String fromTimeUnit,
        @QueryParam("toTimeAgo") @DefaultValue("0") long toTimeAgo,
        @QueryParam("toTimeUnit") @DefaultValue("HOURS") String toTimeUnit,
        @QueryParam("featureFields") @DefaultValue("user activityType, activityType context") String featureFields,
        @QueryParam("filters") @DefaultValue("activityType:0, user:3 2000") String filters,
        @QueryParam("desiredNumberOfResults") @DefaultValue("1000") int desiredNumberOfResults,
        @QueryParam("logLevel") @DefaultValue("NONE") String logLevel) {

        String rendered = toolsService.renderPlugin(catwalkPluginRegion,
            Optional.of(new CatwalkPluginRegionInput(
                tenantId,
                fromTimeAgo,
                fromTimeUnit,
                toTimeAgo,
                toTimeUnit,
                featureFields,
                filters.trim(),
                desiredNumberOfResults,
                logLevel)));
        return Response.ok(rendered).build();
    }
}
