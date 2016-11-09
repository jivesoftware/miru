package com.jivesoftware.os.miru.tools.deployable.endpoints;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.tools.deployable.MiruToolsService;
import com.jivesoftware.os.miru.tools.deployable.region.RealwaveFramePluginRegion;
import com.jivesoftware.os.miru.tools.deployable.region.RealwavePluginRegion;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.shared.ResponseHelper;
import java.util.Map;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
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
@Path("/ui/tools/realwave")
public class RealwavePluginEndpoints {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruToolsService toolsService;
    private final RealwavePluginRegion realwavePluginRegion;
    private final RealwaveFramePluginRegion realwaveFramePluginRegion;

    private final ResponseHelper responseHelper = ResponseHelper.INSTANCE;

    public RealwavePluginEndpoints(@Context MiruToolsService toolsService,
        @Context RealwavePluginRegion realwavePluginRegion,
        @Context RealwaveFramePluginRegion realwaveFramePluginRegion) {
        this.toolsService = toolsService;
        this.realwavePluginRegion = realwavePluginRegion;
        this.realwaveFramePluginRegion = realwaveFramePluginRegion;
    }

    @GET
    @Produces(MediaType.TEXT_HTML)
    public Response getRealwave(@QueryParam("tenantId") @DefaultValue("") String tenantId,
        @QueryParam("lookbackSeconds") @DefaultValue("300") int lookbackSeconds,
        @QueryParam("buckets") @DefaultValue("30") int buckets,
        @QueryParam("field1") @DefaultValue("activityType") String field1,
        @QueryParam("terms1") @DefaultValue("0, 1, 11, 65") String terms1,
        @QueryParam("field2") @DefaultValue("") String field2,
        @QueryParam("terms2") @DefaultValue("") String terms2,
        @QueryParam("filters") @DefaultValue("") String filters,
        @QueryParam("graphType") @DefaultValue("Line") String graphType) {
        String rendered = toolsService.renderPlugin(realwavePluginRegion,
            Optional.of(new RealwavePluginRegion.RealwavePluginRegionInput(
                tenantId,
                -1,
                lookbackSeconds,
                buckets,
                field1.trim(),
                terms1.trim(),
                field2.trim(),
                terms2.trim(),
                filters.trim(),
                graphType.trim(),
                true,
                480,
                320,
                true)));
        return Response.ok(rendered).build();
    }

    @GET
    @Path("/frame")
    @Produces(MediaType.TEXT_HTML)
    public Response getRealwaveFrame(@QueryParam("tenantId") @DefaultValue("") String tenantId,
        @QueryParam("lookbackSeconds") @DefaultValue("300") int lookbackSeconds,
        @QueryParam("buckets") @DefaultValue("30") int buckets,
        @QueryParam("field1") @DefaultValue("activityType") String field1,
        @QueryParam("terms1") @DefaultValue("0, 1, 11, 65") String terms1,
        @QueryParam("field2") @DefaultValue("") String field2,
        @QueryParam("terms2") @DefaultValue("") String terms2,
        @QueryParam("filters") @DefaultValue("") String filters,
        @QueryParam("graphType") @DefaultValue("Line") String graphType,
        @QueryParam("legend") @DefaultValue("false") boolean legend,
        @QueryParam("width") @DefaultValue("480") int width,
        @QueryParam("height") @DefaultValue("320") int height,
        @QueryParam("requireFocus") @DefaultValue("false") boolean requireFocus) {
        String rendered = toolsService.renderFramePlugin(realwaveFramePluginRegion,
            Optional.of(new RealwavePluginRegion.RealwavePluginRegionInput(
                tenantId,
                -1,
                lookbackSeconds,
                buckets,
                field1.trim(),
                terms1.trim(),
                field2.trim(),
                terms2.trim(),
                filters.trim(),
                graphType.trim(),
                legend,
                width,
                height,
                requireFocus)));
        return Response.ok(rendered).build();
    }

    @POST
    @Path("/poll")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public Response pollRealwave(@FormParam("tenantId") @DefaultValue("") String tenantId,
        @FormParam("startTimestamp") @DefaultValue("0") long startTimestamp,
        @FormParam("lookbackSeconds") @DefaultValue("300") int lookbackSeconds,
        @FormParam("buckets") @DefaultValue("30") int buckets,
        @FormParam("field1") @DefaultValue("activityType") String field1,
        @FormParam("terms1") @DefaultValue("0, 1, 11, 65") String terms1,
        @FormParam("field2") @DefaultValue("") String field2,
        @FormParam("terms2") @DefaultValue("") String terms2,
        @FormParam("filters") @DefaultValue("") String filters) {
        try {
            Map<String, Object> result = realwavePluginRegion.poll(
                new RealwavePluginRegion.RealwavePluginRegionInput(
                    tenantId,
                    startTimestamp,
                    lookbackSeconds,
                    buckets,
                    field1.trim(),
                    terms1.trim(),
                    field2.trim(),
                    terms2.trim(),
                    filters.trim(),
                    null,
                    false,
                    -1,
                    -1,
                    false));
            return responseHelper.jsonResponse(result != null ? result : "");
        } catch (Exception e) {
            LOG.error("Realwave poll failed", e);
            return responseHelper.errorResponse("Realwave poll failed", e);
        }
    }
}
