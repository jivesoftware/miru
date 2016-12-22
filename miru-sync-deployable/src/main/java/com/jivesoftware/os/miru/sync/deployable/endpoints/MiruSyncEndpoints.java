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

import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.sync.deployable.MiruSyncCopier;
import com.jivesoftware.os.miru.sync.deployable.MiruSyncSender;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.shared.ResponseHelper;
import java.nio.charset.StandardCharsets;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

/**
 * @author jonathan
 */
@Singleton
@Path("/miru/sync")
public class MiruSyncEndpoints {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final ResponseHelper responseHelper = ResponseHelper.INSTANCE;
    private final MiruSyncSender<?, ?> syncSender;
    private final MiruSyncCopier<?, ?> syncCopier;
    private final MiruStats miruStats;

    public MiruSyncEndpoints(@Context MiruSyncSender<?, ?> syncSender,
        @Context MiruSyncCopier<?, ?> syncCopier,
        @Context MiruStats miruStats) {
        this.syncSender = syncSender;
        this.syncCopier = syncCopier;
        this.miruStats = miruStats;
    }

    @GET
    @Path("/")
    @Produces(MediaType.APPLICATION_JSON)
    public Response get() {
        try {
            return responseHelper.jsonResponse("Success");
        } catch (Exception e) {
            LOG.error("Failed to get.", e);
            return Response.serverError().build();
        }
    }

    @POST
    @Path("/reset/{tenantId}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response postReset(@PathParam("tenantId") String tenantId) {
        try {
            if (syncSender != null) {
                boolean result = syncSender.resetProgress(new MiruTenantId(tenantId.getBytes(StandardCharsets.UTF_8)));
                return Response.ok(result).build();
            } else {
                return Response.status(Status.SERVICE_UNAVAILABLE).entity("Sender is not enabled").build();
            }
        } catch (Exception e) {
            LOG.error("Failed to reset.", e);
            return Response.serverError().build();
        }
    }

    @POST
    @Path("/copy/local/{tenantId}/{fromPartitionId}/{toPartitionId}/{fromTimestamp}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response copyLocal(@PathParam("tenantId") String tenantId,
        @PathParam("fromPartitionId") int fromPartitionId,
        @PathParam("toPartitionId") int toPartitionId,
        @PathParam("fromTimestamp") long fromTimestamp) {
        try {
            if (syncCopier != null) {
                int copied = syncCopier.copyLocal(new MiruTenantId(tenantId.getBytes(StandardCharsets.UTF_8)),
                    MiruPartitionId.of(fromPartitionId),
                    MiruPartitionId.of(toPartitionId),
                    fromTimestamp);
                return Response.ok("Copied " + copied).build();
            } else {
                return Response.status(Status.SERVICE_UNAVAILABLE).entity("Copier is not enabled").build();
            }
        } catch (Exception e) {
            LOG.error("Failed to copy.", e);
            return Response.serverError().build();
        }
    }

}
