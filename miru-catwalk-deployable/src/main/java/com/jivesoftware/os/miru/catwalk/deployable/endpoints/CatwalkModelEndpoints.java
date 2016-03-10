package com.jivesoftware.os.miru.catwalk.deployable.endpoints;

import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.catwalk.deployable.CatwalkModelQueue;
import com.jivesoftware.os.miru.catwalk.deployable.CatwalkModelService;
import com.jivesoftware.os.miru.stream.plugins.catwalk.CatwalkModel;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.nio.charset.StandardCharsets;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 *
 */
@Singleton
@Path("/miru/catwalk/model")
public class CatwalkModelEndpoints {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final CatwalkModelService catwalkModelService;
    private final CatwalkModelQueue catwalkModelQueue;

    public CatwalkModelEndpoints(@Context CatwalkModelService catwalkModelService,
        @Context CatwalkModelQueue catwalkModelQueue) {
        this.catwalkModelService = catwalkModelService;
        this.catwalkModelQueue = catwalkModelQueue;
    }

    @GET
    @Path("/{tenantId}/{userId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getModel(@PathParam("tenantId") String tenantId,
        @PathParam("userId") String userId,
        String[][] featureFieldIds) {
        try {
            CatwalkModel model = catwalkModelService.getModel(new MiruTenantId(tenantId.getBytes(StandardCharsets.UTF_8)), userId, featureFieldIds);
            return Response.ok(model).build();
        } catch (Exception e) {
            LOG.error("Failed to get model for {} {}", new Object[]{tenantId, userId}, e);
            return Response.serverError().entity("Failed to get model").build();
        }
    }

    @POST
    @Path("/{tenantId}/{userId}/{partitionId}")
    @Produces(MediaType.TEXT_HTML)
    public Response updateModel(@PathParam("tenantId") String tenantId,
        @PathParam("userId") String userId,
        @PathParam("partitionId") int partitionId,
        String[][] featureFieldIds) {
        try {
            catwalkModelQueue.updateModel(new MiruTenantId(tenantId.getBytes(StandardCharsets.UTF_8)),
                userId,
                featureFieldIds,
                partitionId);
            return Response.ok("success").build();
        } catch (Exception e) {
            LOG.error("Failed to update model for {} {} {}", new Object[]{tenantId, userId, partitionId}, e);
            return Response.serverError().entity("Failed to update model").build();
        }
    }

}
