package com.jivesoftware.os.miru.manage.deployable;

import com.google.common.base.Optional;
import com.jivesoftware.os.jive.utils.jaxrs.util.ResponseHelper;
import com.jivesoftware.os.miru.manage.deployable.region.RealwavePluginRegion;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Map;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
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
@Path("/miru/manage/realwave")
public class RealwavePluginEndpoints {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruManageService miruManageService;
    private final RealwavePluginRegion realwavePluginRegion;

    private final ResponseHelper responseHelper = ResponseHelper.INSTANCE;

    public RealwavePluginEndpoints(@Context MiruManageService miruManageService, @Context RealwavePluginRegion realwavePluginRegion) {
        this.miruManageService = miruManageService;
        this.realwavePluginRegion = realwavePluginRegion;
    }

    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response getRealwave(@QueryParam("tenantId") @DefaultValue("") String tenantId,
        @QueryParam("lookbackSeconds") @DefaultValue("300") int lookbackSeconds,
        @QueryParam("buckets") @DefaultValue("30") int buckets,
        @QueryParam("field1") @DefaultValue("activityType") String field1,
        @QueryParam("terms1") @DefaultValue("0, 1, 11, 65") String terms1,
        @QueryParam("field2") @DefaultValue("") String field2,
        @QueryParam("terms2") @DefaultValue("") String terms2,
        @QueryParam("filters") @DefaultValue("") String filters) {
        String rendered = miruManageService.renderPlugin(realwavePluginRegion,
            Optional.of(new RealwavePluginRegion.RealwavePluginRegionInput(
                tenantId,
                -1,
                lookbackSeconds,
                buckets,
                field1.trim(),
                terms1.trim(),
                field2.trim(),
                terms2.trim(),
                filters.trim())));
        return Response.ok(rendered).build();
    }

    @POST
    @Path("/poll")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public Response pollRealwave(@QueryParam("tenantId") @DefaultValue("") String tenantId,
        @QueryParam("startTimestamp") @DefaultValue("0") long startTimestamp,
        @QueryParam("lookbackSeconds") @DefaultValue("300") int lookbackSeconds,
        @QueryParam("buckets") @DefaultValue("30") int buckets,
        @QueryParam("field1") @DefaultValue("activityType") String field1,
        @QueryParam("terms1") @DefaultValue("0, 1, 11, 65") String terms1,
        @QueryParam("field2") @DefaultValue("") String field2,
        @QueryParam("terms2") @DefaultValue("") String terms2,
        @QueryParam("filters") @DefaultValue("") String filters) {
        Map<String, Object> result = null;
        try {
            result = realwavePluginRegion.poll(
                new RealwavePluginRegion.RealwavePluginRegionInput(
                    tenantId,
                    startTimestamp,
                    lookbackSeconds,
                    buckets,
                    field1.trim(),
                    terms1.trim(),
                    field2.trim(),
                    terms2.trim(),
                    filters.trim()));
            return responseHelper.jsonResponse(result != null ? result : "");
        } catch (Exception e) {
            LOG.error("Realwave poll failed", e);
            return responseHelper.errorResponse("Realwave poll failed", e);
        }
    }
}
