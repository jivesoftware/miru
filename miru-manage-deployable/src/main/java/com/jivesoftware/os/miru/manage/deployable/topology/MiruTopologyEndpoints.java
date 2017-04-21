package com.jivesoftware.os.miru.manage.deployable.topology;

import com.google.common.base.Charsets;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.topology.MiruHeartbeatRequest;
import com.jivesoftware.os.miru.api.topology.MiruIngressUpdate;
import com.jivesoftware.os.miru.cluster.MiruRegistryClusterClient;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.shared.ResponseHelper;
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
    private final MiruStats stats;

    public MiruTopologyEndpoints(@Context MiruRegistryClusterClient registry,
        @Context MiruStats stats) {
        this.registry = registry;
        this.stats = stats;
    }

    @GET
    @Path("/routing/{tenantId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getRouting(@PathParam("tenantId") String tenantId) {
        try {
            long start = System.currentTimeMillis();
            Response r = ResponseHelper.INSTANCE.jsonResponse(registry.routingTopology(new MiruTenantId(tenantId.getBytes(StandardCharsets.UTF_8))));
            stats.ingressed("/routing/" + tenantId, 1, System.currentTimeMillis() - start);
            return r;
        } catch (Exception x) {
            String msg = "Failed to getRouting for " + tenantId;
            LOG.error(msg, x);
            return ResponseHelper.INSTANCE.errorResponse(msg, x);
        }
    }

    @POST
    @Path("/update/ingress")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response updateIngress(MiruIngressUpdate update) {
        try {
            long start = System.currentTimeMillis();
            registry.updateIngress(update);
            Response r = ResponseHelper.INSTANCE.jsonResponse("ok");
            stats.ingressed("/update/ingress", 1, System.currentTimeMillis() - start);
            return r;
        } catch (Exception x) {
            String msg = "Failed to update ingress " + update;
            if (LOG.isDebugEnabled()) {
                LOG.debug(msg, x);
            } else {
                LOG.error(msg);
            }
            return ResponseHelper.INSTANCE.errorResponse(msg, x);
        }
    }

    @POST
    @Path("/remove/ingress/{tenantId}/{partitionId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response removeIngress(@PathParam("tenantId") String tenantId,
        @PathParam("partitionId") int partitionId) {
        try {
            long start = System.currentTimeMillis();
            registry.removeIngress(new MiruTenantId(tenantId.getBytes(Charsets.UTF_8)), MiruPartitionId.of(partitionId));
            Response r = ResponseHelper.INSTANCE.jsonResponse("ok");
            stats.ingressed("/remove/ingress/" + tenantId + "/" + partitionId, 1, System.currentTimeMillis() - start);
            return r;
        } catch (Exception x) {
            String msg = "Failed to remove ingress";
            LOG.error(msg, x);
            return ResponseHelper.INSTANCE.errorResponse(msg, x);
        }
    }

    @POST
    @Path("/update/lastTimestamp/{tenantId}/{partitionId}/{host}/{lastTimestamp}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response updateIngress(@PathParam("tenantId") String tenantId,
        @PathParam("partitionId") int partitionId,
        @PathParam("host") String host,
        @PathParam("lastTimestamp") long lastTimestamp) {
        try {
            long start = System.currentTimeMillis();
            MiruPartitionCoord coord = new MiruPartitionCoord(new MiruTenantId(tenantId.getBytes(Charsets.UTF_8)),
                MiruPartitionId.of(partitionId),
                new MiruHost(host));
            registry.updateLastTimestamp(coord, lastTimestamp);
            Response r = ResponseHelper.INSTANCE.jsonResponse("ok");
            stats.ingressed("/update/lastTimestamp", 1, System.currentTimeMillis() - start);
            return r;
        } catch (Exception x) {
            String msg = "Failed to update lastTimestamp for " + tenantId + " " + partitionId + " " + host + " " + lastTimestamp;
            if (LOG.isDebugEnabled()) {
                LOG.debug(msg, x);
            } else {
                LOG.error(msg);
            }
            return ResponseHelper.INSTANCE.errorResponse(msg, x);
        }
    }

    @POST
    @Path("/destroy/partition/{tenantId}/{partitionId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response destroyPartition(@PathParam("tenantId") String tenantId,
        @PathParam("partitionId") int partitionId) {
        try {
            long start = System.currentTimeMillis();
            registry.destroyPartition(new MiruTenantId(tenantId.getBytes(Charsets.UTF_8)), MiruPartitionId.of(partitionId));
            Response r = ResponseHelper.INSTANCE.jsonResponse("ok");
            stats.ingressed("/destroy/partition/" + tenantId + "/" + partitionId, 1, System.currentTimeMillis() - start);
            return r;
        } catch (Exception x) {
            String msg = "Failed to destroy partition";
            LOG.error(msg, x);
            return ResponseHelper.INSTANCE.errorResponse(msg, x);
        }
    }

    @GET
    @Path("/partition/status/{tenantId}/{partitionId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getPartitionStatus(@PathParam("tenantId") String tenantId,
        @PathParam("partitionId") int largestPartitionId) {
        try {
            long start = System.currentTimeMillis();
            Response r = ResponseHelper.INSTANCE.jsonResponse(
                registry.getPartitionStatus(new MiruTenantId(tenantId.getBytes(Charsets.UTF_8)), MiruPartitionId.of(largestPartitionId)));
            stats.ingressed("/partition/status/" + tenantId + "/" + largestPartitionId, 1, System.currentTimeMillis() - start);
            return r;
        } catch (Exception x) {
            String msg = "Failed to get partition status for " + tenantId + " " + largestPartitionId;
            LOG.error(msg, x);
            return ResponseHelper.INSTANCE.errorResponse(msg, x);
        }
    }

    @POST
    @Path("/thumpthump/{logicalName}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response thumpthump(@PathParam("logicalName") String logicalName,
        MiruHeartbeatRequest request) {
        try {
            long start = System.currentTimeMillis();
            MiruHost miruHost = new MiruHost(logicalName);
            Response r = ResponseHelper.INSTANCE.jsonResponse(registry.thumpthump(miruHost, request));
            stats.ingressed("/thumpthump/" + logicalName, 1, System.currentTimeMillis() - start);
            return r;
        } catch (Exception x) {
            String msg = "Failed to thumpthump for " + logicalName;
            if (LOG.isDebugEnabled()) {
                LOG.debug(msg, x);
            } else {
                LOG.error(msg + ": " + x.getMessage());
            }
            return ResponseHelper.INSTANCE.errorResponse(msg, x);
        }
    }

    @POST
    @Path("/allHosts")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAllHosts() {
        try {
            long start = System.currentTimeMillis();
            Response r = ResponseHelper.INSTANCE.jsonResponse(registry.allhosts());
            stats.ingressed("/allHosts", 1, System.currentTimeMillis() - start);
            return r;
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
            long start = System.currentTimeMillis();
            Response r = ResponseHelper.INSTANCE.jsonResponse(registry.tenantConfig(new MiruTenantId(tenantId.getBytes(StandardCharsets.UTF_8))));
            stats.ingressed("/tenantConfig/" + tenantId, 1, System.currentTimeMillis() - start);
            return r;
        } catch (Exception x) {
            String msg = "Failed to getTenantConfig for " + tenantId;
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
            long start = System.currentTimeMillis();
            Response r = ResponseHelper.INSTANCE.jsonResponse(registry.partitions(new MiruTenantId(tenantId.getBytes(StandardCharsets.UTF_8))));
            stats.ingressed("/partitions/" + tenantId, 1, System.currentTimeMillis() - start);
            return r;
        } catch (Exception x) {
            String msg = "Failed to getPartitionsForTenant for " + tenantId;
            LOG.error(msg, x);
            return ResponseHelper.INSTANCE.errorResponse(msg, x);
        }
    }

    @GET
    @Path("/ingress/ranges/{tenantId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getIngressRanges(@PathParam("tenantId") String tenantId) {
        try {
            long start = System.currentTimeMillis();
            Response r = ResponseHelper.INSTANCE.jsonResponse(registry.getIngressRanges(new MiruTenantId(tenantId.getBytes(StandardCharsets.UTF_8))));
            stats.ingressed("/ingress/ranges/" + tenantId, 1, System.currentTimeMillis() - start);
            return r;
        } catch (Exception x) {
            String msg = "Failed to getIngressRanges for " + tenantId;
            if (LOG.isDebugEnabled()) {
                LOG.debug(msg, x);
            } else {
                LOG.error(msg);
            }
            return ResponseHelper.INSTANCE.errorResponse(msg, x);
        }
    }

    @POST
    @Path("/remove/{logicalName}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response remove(@PathParam("logicalName") String logicalName,
        MiruHeartbeatRequest request) {
        try {
            long start = System.currentTimeMillis();
            MiruHost miruHost = new MiruHost(logicalName);
            registry.removeHost(miruHost);
            stats.ingressed("/remove/" + logicalName, 1, System.currentTimeMillis() - start);
            return ResponseHelper.INSTANCE.jsonResponse("");
        } catch (Exception x) {
            String msg = "Failed to removeHost for " + logicalName;
            LOG.error(msg, x);
            return ResponseHelper.INSTANCE.errorResponse(msg, x);
        }
    }

    @POST
    @Path("/remove/{logicalName}/{tenantId}/{partitionId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response removeTopology(@PathParam("logicalName") String logicalName,
        @PathParam("tenantId") String tenantId,
        @PathParam("partitionId") int partitionId) {
        try {
            long start = System.currentTimeMillis();
            MiruTenantId miruTenantId = new MiruTenantId(tenantId.getBytes(StandardCharsets.UTF_8));
            MiruPartitionId miruPartitionId = MiruPartitionId.of(partitionId);
            MiruHost miruHost = new MiruHost(logicalName);
            registry.removeTopology(miruHost, miruTenantId, miruPartitionId);
            stats.ingressed("/remove/" + logicalName + "/" + tenantId + "/" + partitionId, 1, System.currentTimeMillis() - start);
            return ResponseHelper.INSTANCE.jsonResponse("");
        } catch (Exception x) {
            String msg = "Failed to removeTopology for " + logicalName + " " + tenantId + " " + partitionId;
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
            long start = System.currentTimeMillis();
            Response r = ResponseHelper.INSTANCE.jsonResponse(registry.getSchema(new MiruTenantId(tenantId.getBytes(StandardCharsets.UTF_8))));
            stats.ingressed("/get/schema/" + tenantId, 1, System.currentTimeMillis() - start);
            return r;
        } catch (Exception x) {
            String msg = "Failed to getSchema for " + tenantId;
            if (LOG.isDebugEnabled()) {
                LOG.debug(msg, x);
            } else {
                LOG.error(msg);
            }
            return ResponseHelper.INSTANCE.errorResponse(msg, x);
        }
    }

    @POST
    @Path("/schema/{tenantId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response registerSchema(@PathParam("tenantId") String tenantId, MiruSchema schema) {
        try {
            long start = System.currentTimeMillis();
            registry.registerSchema(new MiruTenantId(tenantId.getBytes(StandardCharsets.UTF_8)), schema);
            stats.ingressed("/schema/" + tenantId, 1, System.currentTimeMillis() - start);
            return ResponseHelper.INSTANCE.jsonResponse("");
        } catch (Exception x) {
            String msg = "Failed to registerSchema for " + tenantId;
            LOG.error(msg, x);
            return ResponseHelper.INSTANCE.errorResponse(msg, x);
        }
    }
}
