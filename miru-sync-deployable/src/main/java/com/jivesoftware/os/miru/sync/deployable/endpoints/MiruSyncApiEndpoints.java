/*
 * Copyright 2014 Jive Software Inc.
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
package com.jivesoftware.os.miru.sync.deployable.endpoints;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.sync.deployable.MiruSyncReceiver;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.shared.ResponseHelper;
import java.io.InputStream;
import java.util.ArrayList;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.xerial.snappy.SnappyInputStream;


/**
 * @author jonathan
 */
@Singleton
@Path("/api/sync/v1")
public class MiruSyncApiEndpoints {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruSyncReceiver syncReceiver;
    private final ObjectMapper mapper;
    private final MiruStats miruStats;

    private final ResponseHelper responseHelper = ResponseHelper.INSTANCE;

    public MiruSyncApiEndpoints(@Context MiruSyncReceiver syncReceiver, @Context ObjectMapper mapper, @Context MiruStats miruStats) {
        this.syncReceiver = syncReceiver;
        this.mapper = mapper;
        this.miruStats = miruStats;
    }

    @POST
    @Path("/write/activities/{tenantId}/{partitionId}")
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.APPLICATION_JSON)
    public Response writeActivity(@PathParam("tenantId") String tenantId,
        @PathParam("partitionId") int partitionId,
        InputStream inputStream) throws Exception {
        PartitionedActivities partitionedActivities;
        try {
            partitionedActivities = mapper.readValue(new SnappyInputStream(inputStream), PartitionedActivities.class);
        } catch (Exception x) {
            LOG.error("Failed decompressing writeActivity({})",
                new Object[] { tenantId }, x);
            return responseHelper.errorResponse("Server error", x);
        }
        try {
            long start = System.currentTimeMillis();
            syncReceiver.writeActivity(new MiruTenantId(tenantId.getBytes(Charsets.UTF_8)), MiruPartitionId.of(partitionId), partitionedActivities);
            miruStats.ingressed("/write/activities/" + tenantId, 1, System.currentTimeMillis() - start);
            return responseHelper.jsonResponse("ok");
        } catch (Exception x) {
            LOG.error("Failed calling writeActivity({},count:{})",
                new Object[] { tenantId, partitionedActivities != null ? partitionedActivities.size() : null }, x);
            return responseHelper.errorResponse("Server error", x);
        }
    }

    private static class PartitionedActivities extends ArrayList<MiruPartitionedActivity> {

    }

    @POST
    @Path("/register/schema/{tenantId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response registerSchema(@PathParam("tenantId") String tenantId,
        MiruSchema schema) throws Exception {
        try {
            long start = System.currentTimeMillis();
            syncReceiver.registerSchema(new MiruTenantId(tenantId.getBytes(Charsets.UTF_8)), schema);
            miruStats.ingressed("/register/schema/" + tenantId, 1, System.currentTimeMillis() - start);
            return responseHelper.jsonResponse("ok");
        } catch (Exception x) {
            LOG.error("Failed calling registerSchema({})",
                new Object[] { tenantId }, x);
            return responseHelper.errorResponse("Server error", x);
        }
    }


}
