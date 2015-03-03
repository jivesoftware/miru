package com.jivesoftware.os.miru.manage.deployable.topology;

import com.jivesoftware.os.jive.utils.jaxrs.util.ResponseHelper;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.topology.MiruHeartbeatRequest;
import com.jivesoftware.os.miru.cluster.MiruRegistryClusterClient;
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

@Singleton
@Path("/miru/topology")
public class MiruTopologyEndpoints {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruRegistryClusterClient registry;

    public MiruTopologyEndpoints(@Context MiruRegistryClusterClient registry) {
        this.registry = registry;
    }

    @GET
    @Path("/routing/{tenantId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getRouting(@PathParam("tenantId") String tenantId) {
        try {
            return ResponseHelper.INSTANCE.jsonResponse(registry.routingTopology(new MiruTenantId(tenantId.getBytes(StandardCharsets.UTF_8))));
        } catch (Exception x) {
            String msg = "Failed to getRouting for " + tenantId;
            LOG.error(msg, x);
            return ResponseHelper.INSTANCE.errorResponse(msg, x);
        }
    }

    @POST
    @Path("/thumpthump/{host}/{port}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response thumpthump(@PathParam("host") String host,
        @PathParam("port") int port, MiruHeartbeatRequest request) {
        try {
            MiruHost miruHost = new MiruHost(host, port);
            return ResponseHelper.INSTANCE.jsonResponse(registry.thumpthump(miruHost, request));
        } catch (Exception x) {
            String msg = "Failed to thumpthump for " + host + ":" + port;
            LOG.error(msg, x);
            return ResponseHelper.INSTANCE.errorResponse(msg, x);
        }
    }

    @POST
    @Path("/allHosts")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAllHosts() {
        try {
            return ResponseHelper.INSTANCE.jsonResponse(registry.allhosts());
        } catch (Exception x) {
            String msg = "Failed to getAllHosts";
            LOG.error(msg, x);
            return ResponseHelper.INSTANCE.errorResponse(msg, x);
        }
    }

    @GET
    @Path("/tenantConfig/{tenantId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getTenantConfig(@PathParam("tenantId") String tenantId) {
        try {
            return ResponseHelper.INSTANCE.jsonResponse(registry.tenantConfig(new MiruTenantId(tenantId.getBytes(StandardCharsets.UTF_8))));
        } catch (Exception x) {
            String msg = "Failed to getTenantConfig for " + tenantId;
            LOG.error(msg, x);
            return ResponseHelper.INSTANCE.errorResponse(msg, x);
        }
    }

    @POST
    @Path("/elect/{host}/{port}/{tenantId}/{partitionId}/{electionId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response addToReplicaRegistry(@PathParam("host") String host,
        @PathParam("port") int port,
        @PathParam("tenantId") String tenantId,
        @PathParam("partitionId") int partitionId,
        @PathParam("electionId") long electionId) {
        try {
            MiruTenantId miruTenantId = new MiruTenantId(tenantId.getBytes(StandardCharsets.UTF_8));
            MiruPartitionId miruPartitionId = MiruPartitionId.of(partitionId);
            MiruHost miruHost = new MiruHost(host, port);
            registry.elect(miruHost, miruTenantId, miruPartitionId, electionId);
            return ResponseHelper.INSTANCE.jsonResponse("");
        } catch (Exception x) {
            String msg = "Failed to addToReplicaRegistry for " + tenantId;
            LOG.error(msg, x);
            return ResponseHelper.INSTANCE.errorResponse(msg, x);
        }
    }

    @POST
    @Path("/remove/{tenantId}/{partitionId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response removeTenantPartionReplicaSet(@PathParam("tenantId") String tenantId,
        @PathParam("partitionId") int partitionId) {
        try {
            registry.removeReplica(new MiruTenantId(tenantId.getBytes(StandardCharsets.UTF_8)),
                MiruPartitionId.of(partitionId));
            return ResponseHelper.INSTANCE.jsonResponse("");
        } catch (Exception x) {
            String msg = "Failed to addToReplicaRegistry for " + tenantId;
            LOG.error(msg, x);
            return ResponseHelper.INSTANCE.errorResponse(msg, x);
        }
    }

    @POST
    @Path("/partitions/{tenantId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getPartitionsForTenant(@PathParam("tenantId") String tenantId) {
        try {
            return ResponseHelper.INSTANCE.jsonResponse(registry.partitions(new MiruTenantId(tenantId.getBytes(StandardCharsets.UTF_8))));
        } catch (Exception x) {
            String msg = "Failed to getPartitionsForTenant for " + tenantId;
            LOG.error(msg, x);
            return ResponseHelper.INSTANCE.errorResponse(msg, x);
        }
    }

    @GET
    @Path("/replicas/{tenantId}/{partitionId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getReplicaSets(@PathParam("tenantId") String tenantId, @PathParam("partitionId") int partitionId) {
        try {
            MiruPartitionId miruPartitionId = MiruPartitionId.of(partitionId);
            return ResponseHelper.INSTANCE.jsonResponse(registry.replicas(new MiruTenantId(tenantId.getBytes(StandardCharsets.UTF_8)),
                miruPartitionId));
        } catch (Exception x) {
            String msg = "Failed to getReplicaSets for " + tenantId + "/" + partitionId;
            LOG.error(msg, x);
            return ResponseHelper.INSTANCE.errorResponse(msg, x);
        }
    }

    @POST
    @Path("/remove/{host}/{port}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response remove(@PathParam("host") String host,
        @PathParam("port") int port, MiruHeartbeatRequest request) {
        try {
            MiruHost miruHost = new MiruHost(host, port);
            registry.remove(miruHost);
            return ResponseHelper.INSTANCE.jsonResponse("");
        } catch (Exception x) {
            String msg = "Failed to removeHost for " + host + ":" + port;
            LOG.error(msg, x);
            return ResponseHelper.INSTANCE.errorResponse(msg, x);
        }
    }

    @POST
    @Path("/remove/{host}/{port}/{tenantId}/{partitionId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response removeTopology(@PathParam("host") String host,
        @PathParam("port") int port,
        @PathParam("tenantId") String tenantId,
        @PathParam("partitionId") int partitionId) {
        try {
            MiruTenantId miruTenantId = new MiruTenantId(tenantId.getBytes(StandardCharsets.UTF_8));
            MiruPartitionId miruPartitionId = MiruPartitionId.of(partitionId);
            MiruHost miruHost = new MiruHost(host, port);
            registry.remove(miruHost, miruTenantId, miruPartitionId);
            return ResponseHelper.INSTANCE.jsonResponse("");
        } catch (Exception x) {
            String msg = "Failed to removeTopology for " + host + ":" + port + " " + tenantId + " " + partitionId;
            LOG.error(msg, x);
            return ResponseHelper.INSTANCE.errorResponse(msg, x);
        }
    }

    @GET
    @Path("/schema/{tenantId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getSchema(@PathParam("tenantId") String tenantId) {
        try {
            return ResponseHelper.INSTANCE.jsonResponse(registry.getSchema(new MiruTenantId(tenantId.getBytes(StandardCharsets.UTF_8))));
        } catch (Exception x) {
            String msg = "Failed to getSchema for " + tenantId;
            LOG.error(msg, x);
            return ResponseHelper.INSTANCE.errorResponse(msg, x);
        }
    }

    @POST
    @Path("/schema/{tenantId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response registerSchema(@PathParam("tenantId") String tenantId, MiruSchema schema) {
        try {
            registry.registerSchema(new MiruTenantId(tenantId.getBytes(StandardCharsets.UTF_8)), schema);
            return ResponseHelper.INSTANCE.jsonResponse("");
        } catch (Exception x) {
            String msg = "Failed to getSchema for " + tenantId;
            LOG.error(msg, x);
            return ResponseHelper.INSTANCE.errorResponse(msg, x);
        }
    }
}
