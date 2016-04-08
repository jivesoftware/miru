package com.jivesoftware.os.miru.tools.deployable.endpoints;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.stream.plugins.strut.StrutQuery.Strategy;
import com.jivesoftware.os.miru.tools.deployable.MiruToolsService;
import com.jivesoftware.os.miru.tools.deployable.region.StrutPluginRegion;
import com.jivesoftware.os.miru.tools.deployable.region.StrutPluginRegion.StrutPluginRegionInput;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
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
@Path("/miru/tools/strut")
public class StrutPluginEndpoints {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruToolsService toolsService;
    private final StrutPluginRegion region;

    public StrutPluginEndpoints(@Context MiruToolsService toolsService, @Context StrutPluginRegion region) {
        this.toolsService = toolsService;
        this.region = region;
    }

    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response getCatwalkModel(@QueryParam("tenantId") @DefaultValue("") String tenantId,
        @QueryParam("fromTimeAgo") @DefaultValue("720") long fromTimeAgo,
        @QueryParam("fromTimeUnit") @DefaultValue("HOURS") String fromTimeUnit,
        @QueryParam("toTimeAgo") @DefaultValue("0") long toTimeAgo,
        @QueryParam("toTimeUnit") @DefaultValue("HOURS") String toTimeUnit,
        @QueryParam("catwalkId") @DefaultValue("test") String catwalkId,
        @QueryParam("modelId") @DefaultValue("2000") String modelId,
        @QueryParam("gatherField") @DefaultValue("parent") String gatherField,
        @QueryParam("gatherTermsForFields") @DefaultValue("") String gatherTermsForFields,
        @QueryParam("gatherFilters") @DefaultValue("activityType:0, user:3 2000") String gatherFilters,
        @QueryParam("featureFields") @DefaultValue("activityType context,"
            + "activityType user,"
            + "context user") String featureFields,
        @QueryParam("featureFilters") @DefaultValue("") String featureFilters,
        @QueryParam("constraintField") @DefaultValue("parent") String constraintField,
        @QueryParam("constraintFilters") @DefaultValue("activityType:1|2|65|72") String constraintFilters,
        @QueryParam("strategy") @DefaultValue("MAX") String strategy,
        @QueryParam("usePartitionModelCache") @DefaultValue("false") boolean usePartitionModelCache,
        @QueryParam("desiredNumberOfResults") @DefaultValue("1000") int desiredNumberOfResults,
        @QueryParam("desiredModelSize") @DefaultValue("10000") int desiredModelSize,
        @QueryParam("logLevel") @DefaultValue("NONE") String logLevel) {

        try {
            String rendered = toolsService.renderPlugin(region,
                Optional.of(new StrutPluginRegionInput(
                    tenantId,
                    fromTimeAgo,
                    fromTimeUnit,
                    toTimeAgo,
                    toTimeUnit,
                    catwalkId,
                    modelId,
                    gatherField,
                    gatherTermsForFields.trim(),
                    gatherFilters.trim(),
                    featureFields,
                    featureFilters.trim(),
                    constraintField,
                    constraintFilters.trim(),
                    Strategy.valueOf(strategy),
                    usePartitionModelCache,
                    desiredNumberOfResults,
                    desiredModelSize,
                    logLevel)));
            return Response.ok(rendered).build();
        } catch (Exception x) {
            LOG.error("Failed to generating catwalkModel.", x);
            return Response.serverError().build();
        }
    }
}
