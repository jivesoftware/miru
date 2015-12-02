package com.jivesoftware.os.miru.reader.deployable;

import com.google.common.base.Charsets;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoordInfo;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.service.MiruService;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.shared.ResponseHelper;
import java.util.concurrent.TimeUnit;
import javax.inject.Singleton;
import javax.ws.rs.DELETE;
import javax.ws.rs.FormParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Singleton
@Path("/miru/config")
public class MiruReaderConfigEndpoints {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final MiruService miruService;
    private final MiruStats stats;
    private final TimestampedOrderIdProvider timestampedOrderIdProvider;
    private final ResponseHelper responseHelper = ResponseHelper.INSTANCE;

    public MiruReaderConfigEndpoints(@Context MiruService miruService,
        @Context MiruStats stats,
        @Context TimestampedOrderIdProvider timestampedOrderIdProvider) {
        this.miruService = miruService;
        this.stats = stats;
        this.timestampedOrderIdProvider = timestampedOrderIdProvider;
    }

    @POST
    @Path("/storage/{tenantId}/{partitionId}/{storage}")
    @Produces(MediaType.TEXT_HTML)
    public Response setStorage(
        @PathParam("tenantId") String tenantId,
        @PathParam("partitionId") Integer partitionId,
        @PathParam("storage") String storage) {
        try {
            long start = System.currentTimeMillis();
            miruService.setStorage(
                new MiruTenantId(tenantId.getBytes(Charsets.UTF_8)),
                MiruPartitionId.of(partitionId),
                MiruBackingStorage.valueOf(storage));
            stats.ingressed("POST:/storage/" + tenantId + "/" + partitionId + "/" + storage, 1, System.currentTimeMillis() - start);
            return Response.ok(storage).build();
        } catch (Throwable t) {
            log.error("Failed to set storage to {} for tenant {} partition {}", new Object[]{storage, tenantId, partitionId}, t);
            return Response.serverError().entity(t.getMessage()).build();
        }
    }

    @DELETE
    @Path("/hosts/{logicalName}/{port}")
    @Produces(MediaType.TEXT_HTML)
    public Response removeHost(
        @PathParam("logicalName") String logicalName,
        @PathParam("port") int port) {

        MiruHost host = new MiruHost(logicalName, port);
        try {
            long start = System.currentTimeMillis();
            miruService.removeHost(host);
            stats.ingressed("DELETE:/hosts/" + logicalName + "/" + port, 1, System.currentTimeMillis() - start);
            return Response.ok(host.toStringForm()).build();
        } catch (Throwable t) {
            log.error("Failed to remove host {}", new Object[]{host}, t);
            return Response.serverError().entity(t.getMessage()).build();
        }
    }

    @DELETE
    @Path("/topology/{tenantId}/{partitionId}/{logicalName}/{port}")
    @Produces(MediaType.TEXT_HTML)
    public Response removeTopology(
        @PathParam("tenantId") String tenantId,
        @PathParam("partitionId") Integer partitionId,
        @PathParam("logicalName") String logicalName,
        @PathParam("port") int port) {

        MiruHost host = new MiruHost(logicalName, port);
        try {
            long start = System.currentTimeMillis();
            miruService.removeTopology(
                new MiruTenantId(tenantId.getBytes(Charsets.UTF_8)),
                MiruPartitionId.of(partitionId),
                host);
            stats.ingressed("DELETE:/topology/" + tenantId + "/" + partitionId + "/" + logicalName + "/" + port, 1, System.currentTimeMillis() - start);
            return Response.ok(host.toStringForm()).build();
        } catch (Throwable t) {
            log.error("Failed to remove topology for tenant {} partition {} host {}", new Object[]{tenantId, partitionId, host}, t);
            return Response.serverError().entity(t.getMessage()).build();
        }
    }

    @POST
    @Path("/check/{tenantId}/{partitionId}/{state}/{storage}")
    public Response check(
        @PathParam("tenantId") String tenantId,
        @PathParam("partitionId") Integer partitionId,
        @PathParam("state") MiruPartitionState state,
        @PathParam("storage") MiruBackingStorage storage) {
        try {
            long start = System.currentTimeMillis();
            MiruTenantId tenant = new MiruTenantId(tenantId.getBytes(Charsets.UTF_8));
            MiruPartitionId partition = MiruPartitionId.of(partitionId);
            Response response;
            if (miruService.checkInfo(tenant, partition, new MiruPartitionCoordInfo(state, storage))) {
                response = responseHelper.jsonResponse("Success");
            } else {
                response = Response.status(Response.Status.NOT_FOUND).build();
            }
            stats.ingressed("POST:/check/" + tenantId + "/" + partitionId + "/" + state + "/" + storage, 1, System.currentTimeMillis() - start);
            return response;
        } catch (Throwable t) {
            log.error("Failed to check state for tenant {} partition {}", new Object[]{tenantId, partitionId}, t);
            return Response.serverError().build();
        }
    }

    @POST
    @Path("/rebuild/prioritize/{tenantId}/{partitionId}")
    public Response check(
        @PathParam("tenantId") String tenantId,
        @PathParam("partitionId") Integer partitionId) {
        try {
            long start = System.currentTimeMillis();
            MiruTenantId tenant = new MiruTenantId(tenantId.getBytes(Charsets.UTF_8));
            MiruPartitionId partition = MiruPartitionId.of(partitionId);
            Response response;
            if (miruService.prioritizeRebuild(tenant, partition)) {
                response = responseHelper.jsonResponse("Success");
            } else {
                response = Response.status(Response.Status.NOT_FOUND).build();
            }
            stats.ingressed("DELETE:" + "/rebuild/prioritize/" + tenantId + "/" + partitionId, 1, System.currentTimeMillis() - start);
            return response;
        } catch (Throwable t) {
            log.error("Failed to prioritize rebuild for tenant {} partition {}", new Object[]{tenantId, partitionId}, t);
            return Response.serverError().build();
        }
    }

    @POST
    @Path("/rebuild")
    @Produces(MediaType.TEXT_HTML)
    public Response rebuild(@FormParam("days") int days) {
        try {
            long smallestTimestamp = timestampedOrderIdProvider.getApproximateId(System.currentTimeMillis() - TimeUnit.DAYS.toDays(days));
            MiruTimeRange miruTimeRange = new MiruTimeRange(smallestTimestamp, Long.MAX_VALUE);
            return Response.ok(miruService.rebuildTimeRange(miruTimeRange)).build();
        } catch (Throwable t) {
            log.error("Failed to rebuild for last {} days", new Object[]{days}, t);
            return Response.serverError().build();
        }
    }
}
