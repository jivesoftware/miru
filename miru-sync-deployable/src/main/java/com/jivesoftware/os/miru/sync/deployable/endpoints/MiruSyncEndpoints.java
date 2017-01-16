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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.sync.api.MiruSyncSenderConfig;
import com.jivesoftware.os.miru.sync.api.MiruSyncTenantConfig;
import com.jivesoftware.os.miru.sync.api.MiruSyncTenantTuple;
import com.jivesoftware.os.miru.sync.deployable.MiruSyncConfigStorage;
import com.jivesoftware.os.miru.sync.deployable.MiruSyncCopier;
import com.jivesoftware.os.miru.sync.deployable.MiruSyncSender;
import com.jivesoftware.os.miru.sync.deployable.MiruSyncSenderConfigStorage;
import com.jivesoftware.os.miru.sync.deployable.MiruSyncSenders;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.shared.ResponseHelper;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Map.Entry;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
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

    private final MiruSyncSenderConfigStorage syncSenderConfigStorage;
    private final MiruSyncConfigStorage syncConfigStorage;
    private final ResponseHelper responseHelper = ResponseHelper.INSTANCE;
    private final MiruSyncSenders<?, ?> syncSenders;
    private final MiruSyncCopier<?, ?> syncCopier;
    private final MiruStats miruStats;

    public MiruSyncEndpoints(@Context MiruSyncSenderConfigStorage syncSenderConfigStorage,
        @Context MiruSyncConfigStorage syncConfigStorage,
        @Context MiruSyncSenders<?, ?> syncSenders,
        @Context MiruSyncCopier<?, ?> syncCopier,
        @Context MiruStats miruStats) {
        this.syncSenderConfigStorage = syncSenderConfigStorage;
        this.syncConfigStorage = syncConfigStorage;
        this.syncSenders = syncSenders;
        this.syncCopier = syncCopier;
        this.miruStats = miruStats;
    }

    @GET
    @Path("/syncspace/list")
    @Produces(MediaType.APPLICATION_JSON)
    public Response listNamesSpaces() {
        try {
            Map<String, MiruSyncSenderConfig> all = syncSenderConfigStorage.getAll();
            return Response.ok(all).build();
        } catch (Exception e) {
            LOG.error("Failed to get.", e);
            return Response.serverError().build();
        }
    }


    @POST
    @Path("/syncspace/add/{name}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response addsyncspace(@PathParam("name") String name,
        MiruSyncSenderConfig syncspaceConfig) {
        try {
            syncSenderConfigStorage.multiPut(ImmutableMap.of(name, syncspaceConfig));
            return responseHelper.jsonResponse("Success");
        } catch (Exception e) {
            LOG.error("Failed to get.", e);
            return Response.serverError().build();
        }
    }

    @DELETE
    @Path("/syncspace/delete/{name}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response deletesyncspace(@PathParam("name") String name) {
        try {
            syncSenderConfigStorage.multiRemove(ImmutableList.of(name));
            return responseHelper.jsonResponse("Success");
        } catch (Exception e) {
            LOG.error("Failed to get.", e);
            return Response.serverError().build();
        }
    }


    @GET
    @Path("/list/{syncspaceName}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getSyncing(@PathParam("syncspaceName") String syncspaceName) {
        try {
            Map<MiruSyncTenantTuple, MiruSyncTenantConfig> all = syncConfigStorage.getAll(syncspaceName);
            if (all != null && !all.isEmpty()) {
                Map<String, MiruSyncTenantConfig> map = Maps.newHashMap();
                for (Entry<MiruSyncTenantTuple, MiruSyncTenantConfig> a : all.entrySet()) {
                    map.put(MiruSyncTenantTuple.toKeyString(a.getKey()), a.getValue());
                }
                return Response.ok(map).build();
            }
            return Response.noContent().entity("{}").build();
        } catch (Exception e) {
            LOG.error("Failed to get.", e);
            return Response.serverError().build();
        }
    }

    @POST
    @Path("/add/{syncspaceName}/{fromTenantId}/{toTenantId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response post(@PathParam("syncspaceName") String syncspaceName,
        @PathParam("fromTenantId") String fromTenantId,
        @PathParam("toTenantId") String toTenantId,
        MiruSyncTenantConfig config) {
        try {

            syncConfigStorage.multiPut(syncspaceName, ImmutableMap.of(
                new MiruSyncTenantTuple(
                    new MiruTenantId(fromTenantId.getBytes(StandardCharsets.UTF_8)),
                    new MiruTenantId(toTenantId.getBytes(StandardCharsets.UTF_8))
                ), config));
            return responseHelper.jsonResponse("Success");
        } catch (Exception e) {
            LOG.error("Failed to add.", e);
            return Response.serverError().build();
        }
    }

    @DELETE
    @Path("/delete/{syncspaceName}/{fromTenantId}/{toTenantId}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response delete(@PathParam("syncspace") String syncspaceName,
        @PathParam("fromTenantId") String fromTenantId,
        @PathParam("toTenantId") String toTenantId) {
        try {
            syncConfigStorage.multiRemove(syncspaceName, ImmutableList.of(new MiruSyncTenantTuple(
                new MiruTenantId(fromTenantId.getBytes(StandardCharsets.UTF_8)),
                new MiruTenantId(toTenantId.getBytes(StandardCharsets.UTF_8))
            )));
            return responseHelper.jsonResponse("Success");
        } catch (Exception e) {
            LOG.error("Failed to get.", e);
            return Response.serverError().build();
        }
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
    @Path("/reset/{syncspaceName}/{tenantId}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response postReset(@PathParam("syncspaceName") String syncspaceName,
        @PathParam("tenantId") String tenantId) {
        try {
            if (syncSenders != null) {
                boolean result = false;
                for (MiruSyncSender<?, ?> miruSyncSender : syncSenders.getActiveSenders()) {
                    if (miruSyncSender.getName().equals(syncspaceName)) {
                        result = miruSyncSender.resetProgress(new MiruTenantId(tenantId.getBytes(StandardCharsets.UTF_8)));
                        break;
                    }
                }
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
    @Path("/copy/local/{fromTenantId}/{fromPartitionId}/{toTenantId}/{toPartitionId}/{fromTimestamp}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response copyLocal(@PathParam("fromTenantId") String fromTenantId,
        @PathParam("fromPartitionId") int fromPartitionId,
        @PathParam("toTenantId") String toTenantId,
        @PathParam("toPartitionId") int toPartitionId,
        @PathParam("fromTimestamp") long fromTimestamp) {
        try {
            if (syncCopier != null) {
                int copied = syncCopier.copyLocal(new MiruTenantId(fromTenantId.getBytes(StandardCharsets.UTF_8)),
                    MiruPartitionId.of(fromPartitionId),
                    new MiruTenantId(toTenantId.getBytes(StandardCharsets.UTF_8)),
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
