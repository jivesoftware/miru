package com.jivesoftware.os.miru.writer.deployable;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.wal.MiruWALDirector;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 *
 */
@Singleton
@Path("/miru/writer")
public class MiruWriterEndpoints {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruWriterUIService writerUIService;
    private final MiruWALDirector miruWALDirector;

    public MiruWriterEndpoints(@Context MiruWriterUIService writerUIService,
        @Context MiruWALDirector miruWALDirector) {
        this.writerUIService = writerUIService;
        this.miruWALDirector = miruWALDirector;
    }

    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response get() {
        String rendered = writerUIService.render();
        return Response.ok(rendered).build();
    }

    @GET
    @Path("/lookup")
    @Produces(MediaType.TEXT_HTML)
    public Response getLookup() {
        String rendered = writerUIService.renderLookup();
        return Response.ok(rendered).build();
    }

    @GET
    @Path("/lookup/{tenantId}")
    @Produces(MediaType.TEXT_HTML)
    public Response getActivityWALForTenant(
        @PathParam("tenantId") String tenantId,
        @QueryParam("afterTimestamp") Long afterTimestamp,
        @QueryParam("limit") Integer limit) {
        String rendered = writerUIService.renderLookupWithFocus(
            new MiruTenantId(tenantId.getBytes(Charsets.UTF_8)),
            Optional.fromNullable(afterTimestamp),
            Optional.fromNullable(limit));
        return Response.ok(rendered).build();
    }

    @GET
    @Path("/wal/activity")
    @Produces(MediaType.TEXT_HTML)
    public Response getActivityWALForTenant() {
        String rendered = writerUIService.renderActivityWAL();
        return Response.ok(rendered).build();
    }

    @GET
    @Path("/wal/activity/{tenantId}")
    @Produces(MediaType.TEXT_HTML)
    public Response getActivityWALForTenant(@PathParam("tenantId") String tenantId) {
        String rendered = writerUIService.renderActivityWALWithTenant(new MiruTenantId(tenantId.getBytes(Charsets.UTF_8)));
        return Response.ok(rendered).build();
    }

    @GET
    @Path("/wal/activity/{tenantId}/{partitionId}")
    @Produces(MediaType.TEXT_HTML)
    public Response getActivityWALForTenantPartition(
        @PathParam("tenantId") String tenantId,
        @PathParam("partitionId") int partitionId,
        @QueryParam("sip") Boolean sip,
        @QueryParam("afterTimestamp") Long afterTimestamp,
        @QueryParam("limit") Integer limit) {
        String rendered = writerUIService.renderActivityWALWithFocus(
            new MiruTenantId(tenantId.getBytes(Charsets.UTF_8)),
            MiruPartitionId.of(partitionId),
            Optional.fromNullable(sip),
            Optional.fromNullable(afterTimestamp),
            Optional.fromNullable(limit));
        return Response.ok(rendered).build();
    }

    @GET
    @Path("/wal/read")
    @Produces(MediaType.TEXT_HTML)
    public Response getReadWALForTenant() {
        String rendered = writerUIService.renderReadWAL();
        return Response.ok(rendered).build();
    }

    @GET
    @Path("/wal/read/{tenantId}")
    @Produces(MediaType.TEXT_HTML)
    public Response getReadWALForTenant(@PathParam("tenantId") String tenantId) {
        String rendered = writerUIService.renderReadWALWithTenant(new MiruTenantId(tenantId.getBytes(Charsets.UTF_8)));
        return Response.ok(rendered).build();
    }

    @GET
    @Path("/wal/read/{tenantId}/{streamId}")
    @Produces(MediaType.TEXT_HTML)
    public Response getReadWALForTenantPartition(
        @PathParam("tenantId") String tenantId,
        @PathParam("streamId") String streamId,
        @QueryParam("sip") Boolean sip,
        @QueryParam("afterTimestamp") Long afterTimestamp,
        @QueryParam("limit") Integer limit) {
        String rendered = writerUIService.renderReadWALWithFocus(
            new MiruTenantId(tenantId.getBytes(Charsets.UTF_8)),
            streamId,
            Optional.fromNullable(sip),
            Optional.fromNullable(afterTimestamp),
            Optional.fromNullable(limit));
        return Response.ok(rendered).build();
    }

    @POST
    @Path("/wal/sanitize/{tenantId}/{partitionId}")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Response sanitizeWAL(@PathParam("tenantId") @DefaultValue("") String tenantId,
        @PathParam("partitionId") @DefaultValue("-1") int partitionId) {
        try {
            miruWALDirector.sanitizeActivityWAL(new MiruTenantId(tenantId.getBytes(Charsets.UTF_8)), MiruPartitionId.of(partitionId));
            miruWALDirector.sanitizeActivitySipWAL(new MiruTenantId(tenantId.getBytes(Charsets.UTF_8)), MiruPartitionId.of(partitionId));
            return Response.ok("success").build();
        } catch (Throwable t) {
            LOG.error("POST /wal/sanitize/" + tenantId + "/" + partitionId, t);
            return Response.serverError().entity(t.getMessage()).build();
        }
    }

    @GET
    @Path("/repair")
    @Produces(MediaType.TEXT_HTML)
    public Response getRepair() {
        String rendered = writerUIService.renderRepair();
        return Response.ok(rendered).build();
    }

    @GET
    @Path("/repair/{tenantId}")
    @Produces(MediaType.TEXT_HTML)
    public Response getRepairForTenant(@PathParam("tenantId") String tenantId) {
        String rendered = writerUIService.renderRepairWithTenant(tenantId);
        return Response.ok(rendered).build();
    }

    @POST
    @Path("/repair/repairBoundaries")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Response repairWAL() {
        try {
            miruWALDirector.repairBoundaries();
            return Response.ok("success").build();
        } catch (Throwable t) {
            LOG.error("POST /repair/repairBoundaries", t);
            return Response.serverError().entity(t.getMessage()).build();
        }
    }

    @POST
    @Path("/repair/removePartition/{tenantId}/{partitionId}")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Response removePartition(@PathParam("tenantId") String tenantId,
        @PathParam("partitionId") int partitionId) {
        try {
            miruWALDirector.removePartition(new MiruTenantId(tenantId.getBytes(Charsets.UTF_8)), MiruPartitionId.of(partitionId));
            return Response.ok("success").build();
        } catch (Throwable t) {
            LOG.error("POST /repair/removePartition/{}/{}", new Object[] { tenantId, partitionId }, t);
            return Response.serverError().entity(t.getMessage()).build();
        }
    }

}
