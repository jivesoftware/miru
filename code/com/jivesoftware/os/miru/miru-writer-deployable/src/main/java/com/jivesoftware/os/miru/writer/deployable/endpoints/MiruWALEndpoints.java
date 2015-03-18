/*
 * Copyright 2015 jonathan.colt.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jivesoftware.os.miru.writer.deployable.endpoints;

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
 *
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
    @Path("repair")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response repairActivityWAL() throws Exception {
        try {
            walDirector.repairActivityWAL();
            return responseHelper.jsonResponse("ok");
        } catch (Exception x) {
            log.error("Failed calling repairActivityWAL()", x);
            return responseHelper.errorResponse("Server error", x);
        }
    }

    @POST
    @Path("sanitize/activity/wal/{tenantId}/{partitionId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response sanitizeActivityWAL(@PathParam("tenantId") MiruTenantId tenantId,
        @PathParam("partitionId") MiruPartitionId partitionId) throws Exception {
        try {
            walDirector.sanitizeActivityWAL(tenantId, partitionId);
            return responseHelper.jsonResponse("ok");
        } catch (Exception x) {
            log.error("Failed calling sanitizeActivityWAL({}, {})", new Object[]{tenantId, partitionId}, x);
            return responseHelper.errorResponse("Server error", x);
        }
    }

    @POST
    @Path("sanitize/sip/wal/{tenantId}/{partitionId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response sanitizeActivitySipWAL(@PathParam("tenantId") MiruTenantId tenantId,
        @PathParam("partitionId") MiruPartitionId partitionId) throws Exception {
        try {
            walDirector.sanitizeActivitySipWAL(tenantId, partitionId);
            return responseHelper.jsonResponse("ok");
        } catch (Exception x) {
            log.error("Failed calling sanitizeActivitySipWAL({}, {})", new Object[]{tenantId, partitionId}, x);
            return responseHelper.errorResponse("Server error", x);
        }
    }

    @GET
    @Path("tenants/all")
    @Consumes(MediaType.APPLICATION_JSON)
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
    @Path("largestPartitionId/{tenantId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getLargestPartitionIdAcrossAllWriters(@PathParam("tenantId") MiruTenantId tenantId) throws Exception {
        try {
            MiruPartitionId partitionId = walDirector.getLargestPartitionIdAcrossAllWriters(tenantId);
            return responseHelper.jsonResponse(partitionId);
        } catch (Exception x) {
            log.error("Failed calling getLargestPartitionIdAcrossAllWriters({})", new Object[]{tenantId}, x);
            return responseHelper.errorResponse("Server error", x);
        }
    }

    @POST
    @Path("partition/status/{tenantId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getPartitionStatus(@PathParam("tenantId") MiruTenantId tenantId,
        List<MiruPartitionId> partitionIds) throws Exception {
        try {
            List<MiruActivityWALStatus> partitionStatus = walDirector.getPartitionStatus(tenantId, partitionIds);
            return responseHelper.jsonResponse(partitionStatus);
        } catch (Exception x) {
            log.error("Failed calling getPartitionStatus({},{})", new Object[]{tenantId, partitionIds}, x);
            return responseHelper.errorResponse("Server error", x);
        }
    }

    @GET
    @Path("lookup/activity/{tenantId}/{batchSize}/{afterTimestamp}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response lookupActivity(@PathParam("tenantId") MiruTenantId tenantId,
        @PathParam("batchSize") int batchSize,
        @PathParam("afterTimestamp") long afterTimestamp) throws Exception {
        try {
            List<MiruLookupEntry> lookupActivity = walDirector.lookupActivity(tenantId, afterTimestamp, batchSize);
            return responseHelper.jsonResponse(lookupActivity);
        } catch (Exception x) {
            log.error("Failed calling lookupActivity({},{},{},{})", new Object[]{tenantId, afterTimestamp, batchSize}, x);
            return responseHelper.errorResponse("Server error", x);
        }
    }

    @POST
    @Path("sip/activity/{tenantId}/{partitionId}/{batchSize}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response sipActivity(@PathParam("tenantId") MiruTenantId tenantId,
        @PathParam("partitionId") MiruPartitionId partitionId,
        @PathParam("batchSize") int batchSize,
        SipActivityCursor cursor)
        throws Exception {
        try {
            StreamBatch<MiruWALEntry, SipActivityCursor> sipActivity = walDirector.sipActivity(tenantId, partitionId, cursor, batchSize);
            return responseHelper.jsonResponse(sipActivity);
        } catch (Exception x) {
            log.error("Failed calling sipActivity({},{},{},{})", new Object[]{tenantId, partitionId, batchSize, cursor}, x);
            return responseHelper.errorResponse("Server error", x);
        }
    }

    @POST
    @Path("activity/{tenantId}/{partitionId}/{batchSize}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getActivity(@PathParam("tenantId") MiruTenantId tenantId,
        @PathParam("partitionId") MiruPartitionId partitionId,
        @PathParam("batchSize") int batchSize,
        GetActivityCursor cursor)
        throws Exception {
        try {
            StreamBatch<MiruWALEntry, GetActivityCursor> activity = walDirector.getActivity(tenantId, partitionId, cursor, batchSize);
            return responseHelper.jsonResponse(activity);
        } catch (Exception x) {
            log.error("Failed calling getActivity({},{},{},{})", new Object[]{tenantId, partitionId, batchSize, cursor}, x);
            return responseHelper.errorResponse("Server error", x);
        }
    }

    @POST
    @Path("sip/read/{tenantId}/{streamId}/{batchSize}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response sipRead(@PathParam("tenantId") MiruTenantId tenantId,
        @PathParam("streamId") MiruStreamId streamId,
        @PathParam("batchSize") int batchSize,
        SipReadCursor cursor) throws Exception {
        try {
            StreamBatch<MiruReadSipEntry, SipReadCursor> sipRead = walDirector.sipRead(tenantId, streamId, cursor, batchSize);
            return responseHelper.jsonResponse(sipRead);
        } catch (Exception x) {
            log.error("Failed calling sipRead({},{},{},{})", new Object[]{tenantId, streamId, batchSize, cursor}, x);
            return responseHelper.errorResponse("Server error", x);
        }
    }

    @POST
    @Path("read/{tenantId}/{streamId}/{batchSize}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getRead(@PathParam("tenantId") MiruTenantId tenantId,
        @PathParam("streamId") MiruStreamId streamId,
        @PathParam("batchSize") int batchSize,
        GetReadCursor cursor) throws Exception {
        try {
            StreamBatch<MiruWALEntry, GetReadCursor> read = walDirector.getRead(tenantId, streamId, cursor, batchSize);
            return responseHelper.jsonResponse(read);
        } catch (Exception x) {
            log.error("Failed calling getRead({},{},{},{})", new Object[]{tenantId, streamId, batchSize, cursor}, x);
            return responseHelper.errorResponse("Server error", x);
        }
    }

}
