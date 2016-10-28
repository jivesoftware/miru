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
@Path("/ui/tools/strut")
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
        @QueryParam("unreadStreamId") @DefaultValue("") String unreadStreamId,
        @QueryParam("unreadOnly") @DefaultValue("false") boolean unreadOnly,
        @QueryParam("scorableField") @DefaultValue("parent") String scorableField,
        @QueryParam("numeratorFilters") @DefaultValue("activityType:0, user:3 2000\n" +
            "activityType:1, user:3 2000\n" +
            "activityType:72, user:3 2000") String numeratorFilters,
        @QueryParam("gatherTermsForFields") @DefaultValue("") String gatherTermsForFields,
        @QueryParam("features") @DefaultValue("user-views; activityType, user; activityType:0\n" +
            "user-visible; activityType, user; activityType:1|2|65|72\n" +
            "place-visible; activityType, context; activityType:1|2|65|72\n" +
            "user-place; context, user; activityType:1|2|65|72\n" +
            "mentions; context, user; activityType:20") String features,
        @QueryParam("constraintField") @DefaultValue("parent") String constraintField,
        @QueryParam("constraintFilters") @DefaultValue("activityType:1|2|65|72") String constraintFilters,
        @QueryParam("numeratorStrategy") @DefaultValue("MAX") String numeratorStrategy,
        @QueryParam("featureStrategy") @DefaultValue("MAX") String featureStrategy,
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
                    unreadStreamId,
                    unreadOnly,
                    scorableField,
                    numeratorFilters.trim(),
                    gatherTermsForFields.trim(),
                    /*featureFields,
                    featureFilters.trim(),*/
                    features.trim(),
                    constraintField,
                    constraintFilters.trim(),
                    Strategy.valueOf(numeratorStrategy),
                    Strategy.valueOf(featureStrategy),
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
