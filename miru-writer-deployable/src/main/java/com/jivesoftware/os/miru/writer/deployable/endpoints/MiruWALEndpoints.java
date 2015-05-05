package com.jivesoftware.os.miru.writer.deployable.endpoints;

import com.google.common.base.Charsets;
import com.jivesoftware.os.jive.utils.jaxrs.util.ResponseHelper;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.wal.MiruActivityWALStatus;
import com.jivesoftware.os.miru.api.wal.MiruReadSipEntry;
import com.jivesoftware.os.miru.api.wal.MiruWALClient.GetActivityCursor;
import com.jivesoftware.os.miru.api.wal.MiruWALClient.GetReadCursor;
import com.jivesoftware.os.miru.api.wal.MiruWALClient.MiruLookupEntry;
import com.jivesoftware.os.miru.api.wal.MiruWALClient.SipActivityCursor;
import com.jivesoftware.os.miru.api.wal.MiruWALClient.SipReadCursor;
import com.jivesoftware.os.miru.api.wal.MiruWALClient.StreamBatch;
import com.jivesoftware.os.miru.api.wal.MiruWALEntry;
import com.jivesoftware.os.miru.wal.MiruWALDirector;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.List;
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
 * @author jonathan.colt
 */
@Singleton
@Path("/miru/wal")
public class MiruWALEndpoints {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final MiruWALDirector walDirector;
    private final ResponseHelper responseHelper = ResponseHelper.INSTANCE;

    public MiruWALEndpoints(@Context MiruWALDirector walDirector) {
        this.walDirector = walDirector;
    }

    @POST
    @Path("/repairBoundaries")
    @Produces(MediaType.APPLICATION_JSON)
    public Response repairBoundaries() throws Exception {
        try {
            walDirector.repairBoundaries();
            return responseHelper.jsonResponse("ok");
        } catch (Exception x) {
            log.error("Failed calling repairBoundaries()", x);
            return responseHelper.errorResponse("Server error", x);
        }
    }

    @POST
    @Path("/repairRanges")
    @Produces(MediaType.APPLICATION_JSON)
    public Response repairRanges() throws Exception {
        try {
            walDirector.repairRanges();
            return responseHelper.jsonResponse("ok");
        } catch (Exception x) {
            log.error("Failed calling repairRanges()", x);
            return responseHelper.errorResponse("Server error", x);
        }
    }

    @POST
    @Path("/sanitize/activity/wal/{tenantId}/{partitionId}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response sanitizeActivityWAL(@PathParam("tenantId") String tenantId,
        @PathParam("partitionId") int partitionId) throws Exception {
        try {
            walDirector.sanitizeActivityWAL(new MiruTenantId(tenantId.getBytes(Charsets.UTF_8)), MiruPartitionId.of(partitionId));
            return responseHelper.jsonResponse("ok");
        } catch (Exception x) {
            log.error("Failed calling sanitizeActivityWAL({}, {})", new Object[] { tenantId, partitionId }, x);
            return responseHelper.errorResponse("Server error", x);
        }
    }

    @POST
    @Path("/sanitize/sip/wal/{tenantId}/{partitionId}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response sanitizeActivitySipWAL(@PathParam("tenantId") String tenantId,
        @PathParam("partitionId") int partitionId) throws Exception {
        try {
            walDirector.sanitizeActivitySipWAL(new MiruTenantId(tenantId.getBytes(Charsets.UTF_8)), MiruPartitionId.of(partitionId));
            return responseHelper.jsonResponse("ok");
        } catch (Exception x) {
            log.error("Failed calling sanitizeActivitySipWAL({}, {})", new Object[] { tenantId, partitionId }, x);
            return responseHelper.errorResponse("Server error", x);
        }
    }

    @GET
    @Path("/tenants/all")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAllTenantIds() throws Exception {
        try {
            List<MiruTenantId> allTenantIds = walDirector.getAllTenantIds();
            return responseHelper.jsonResponse(allTenantIds);
        } catch (Exception x) {
            log.error("Failed calling getAllTenantIds()", x);
            return responseHelper.errorResponse("Server error", x);
        }
    }

    @GET
    @Path("/largestPartitionId/{tenantId}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getLargestPartitionIdAcrossAllWriters(@PathParam("tenantId") String tenantId) throws Exception {
        try {
            MiruPartitionId partitionId = walDirector.getLargestPartitionIdAcrossAllWriters(new MiruTenantId(tenantId.getBytes(Charsets.UTF_8)));
            return responseHelper.jsonResponse(partitionId);
        } catch (Exception x) {
            log.error("Failed calling getLargestPartitionIdAcrossAllWriters({})", new Object[] { tenantId }, x);
            return responseHelper.errorResponse("Server error", x);
        }
    }

    @POST
    @Path("/partition/status/{tenantId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getPartitionStatus(@PathParam("tenantId") String tenantId,
        List<MiruPartitionId> partitionIds) throws Exception {
        try {
            List<MiruActivityWALStatus> partitionStatus = walDirector.getPartitionStatus(new MiruTenantId(tenantId.getBytes(Charsets.UTF_8)), partitionIds);
            return responseHelper.jsonResponse(partitionStatus);
        } catch (Exception x) {
            log.error("Failed calling getPartitionStatus({},{})", new Object[] { tenantId, partitionIds }, x);
            return responseHelper.errorResponse("Server error", x);
        }
    }

    @GET
    @Path("/lookup/activity/{tenantId}/{batchSize}/{afterTimestamp}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response lookupActivity(@PathParam("tenantId") String tenantId,
        @PathParam("batchSize") int batchSize,
        @PathParam("afterTimestamp") long afterTimestamp) throws Exception {
        try {
            List<MiruLookupEntry> lookupActivity = walDirector.lookupActivity(new MiruTenantId(tenantId.getBytes(Charsets.UTF_8)), afterTimestamp, batchSize);
            return responseHelper.jsonResponse(lookupActivity);
        } catch (Exception x) {
            log.error("Failed calling lookupActivity({},{},{},{})", new Object[] { tenantId, afterTimestamp, batchSize }, x);
            return responseHelper.errorResponse("Server error", x);
        }
    }

    @POST
    @Path("/sip/activity/{tenantId}/{partitionId}/{batchSize}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response sipActivity(@PathParam("tenantId") String tenantId,
        @PathParam("partitionId") int partitionId,
        @PathParam("batchSize") int batchSize,
        SipActivityCursor cursor)
        throws Exception {
        try {
            StreamBatch<MiruWALEntry, SipActivityCursor> sipActivity = walDirector.sipActivity(new MiruTenantId(tenantId.getBytes(Charsets.UTF_8)),
                MiruPartitionId.of(partitionId), cursor, batchSize);
            return responseHelper.jsonResponse(sipActivity);
        } catch (Exception x) {
            log.error("Failed calling sipActivity({},{},{},{})", new Object[] { tenantId, partitionId, batchSize, cursor }, x);
            return responseHelper.errorResponse("Server error", x);
        }
    }

    @POST
    @Path("/activity/{tenantId}/{partitionId}/{batchSize}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getActivity(@PathParam("tenantId") String tenantId,
        @PathParam("partitionId") int partitionId,
        @PathParam("batchSize") int batchSize,
        GetActivityCursor cursor)
        throws Exception {
        try {
            StreamBatch<MiruWALEntry, GetActivityCursor> activity = walDirector.getActivity(new MiruTenantId(tenantId.getBytes(Charsets.UTF_8)),
                MiruPartitionId.of(partitionId), cursor, batchSize);
            return responseHelper.jsonResponse(activity);
        } catch (Exception x) {
            log.error("Failed calling getActivity({},{},{},{})", new Object[] { tenantId, partitionId, batchSize, cursor }, x);
            return responseHelper.errorResponse("Server error", x);
        }
    }

    @POST
    @Path("/sip/read/{tenantId}/{streamId}/{batchSize}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response sipRead(@PathParam("tenantId") String tenantId,
        @PathParam("streamId") String streamId,
        @PathParam("batchSize") int batchSize,
        SipReadCursor cursor) throws Exception {
        try {
            StreamBatch<MiruReadSipEntry, SipReadCursor> sipRead = walDirector.sipRead(
                new MiruTenantId(tenantId.getBytes(Charsets.UTF_8)), new MiruStreamId(streamId.getBytes(Charsets.UTF_8)), cursor, batchSize);
            return responseHelper.jsonResponse(sipRead);
        } catch (Exception x) {
            log.error("Failed calling sipRead({},{},{},{})", new Object[] { tenantId, streamId, batchSize, cursor }, x);
            return responseHelper.errorResponse("Server error", x);
        }
    }

    @POST
    @Path("/read/{tenantId}/{streamId}/{batchSize}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getRead(@PathParam("tenantId") String tenantId,
        @PathParam("streamId") String streamId,
        @PathParam("batchSize") int batchSize,
        GetReadCursor cursor) throws Exception {
        try {
            StreamBatch<MiruWALEntry, GetReadCursor> read = walDirector.getRead(new MiruTenantId(tenantId.getBytes(Charsets.UTF_8)),
                new MiruStreamId(streamId.getBytes(Charsets.UTF_8)), cursor, batchSize);
            return responseHelper.jsonResponse(read);
        } catch (Exception x) {
            log.error("Failed calling getRead({},{},{},{})", new Object[] { tenantId, streamId, batchSize, cursor }, x);
            return responseHelper.errorResponse("Server error", x);
        }
    }

}
