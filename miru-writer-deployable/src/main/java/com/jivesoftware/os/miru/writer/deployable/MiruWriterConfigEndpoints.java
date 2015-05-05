package com.jivesoftware.os.miru.writer.deployable;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.cluster.client.MiruReplicaSetDirector;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import javax.inject.Singleton;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static com.jivesoftware.os.miru.api.MiruConfigReader.CONFIG_SERVICE_ENDPOINT_PREFIX;

@Singleton
@Path(CONFIG_SERVICE_ENDPOINT_PREFIX)
public class MiruWriterConfigEndpoints {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final MiruReplicaSetDirector replicaSetDirector;

    public MiruWriterConfigEndpoints(@Context MiruReplicaSetDirector replicaSetDirector) {
        this.replicaSetDirector = replicaSetDirector;
    }

    @POST
    @Path("/replicas/{tenantId}/{partitionId}/{logicalName}/{port}")
    @Produces(MediaType.TEXT_HTML)
    public Response moveReplica(
        @PathParam("tenantId") String tenantId,
        @PathParam("partitionId") Integer partitionId,
        @PathParam("logicalName") String logicalName,
        @PathParam("port") int port,
        @QueryParam("fromHost") String fromHost,
        @QueryParam("fromPort") int fromPort) {

        Optional fromHostPort;
        if (fromHost == null) {
            fromHostPort = Optional.absent();
        } else {
            fromHostPort = Optional.of(new MiruHost(fromHost, fromPort));
        }

        MiruHost to = new MiruHost(logicalName, port);
        try {
            replicaSetDirector.moveReplica(
                new MiruTenantId(tenantId.getBytes(Charsets.UTF_8)),
                MiruPartitionId.of(partitionId),
                fromHostPort,
                to);
            return Response.ok(to.toStringForm()).build();
        } catch (Throwable t) {
            log.error("Failed to move replica for tenant {} partition {} from {} to {}", new Object[] { tenantId, partitionId, fromHostPort,  to}, t);
            return Response.serverError().entity(t.getMessage()).build();
        }
    }

    @DELETE
    @Path("/replicas/{tenantId}/{partitionId}")
    @Produces(MediaType.TEXT_HTML)
    public Response removeReplicas(
        @PathParam("tenantId") String tenantId,
        @PathParam("partitionId") Integer partitionId) {
        try {
            replicaSetDirector.removeReplicas(
                new MiruTenantId(tenantId.getBytes(Charsets.UTF_8)),
                MiruPartitionId.of(partitionId));
            return Response.ok(partitionId.toString()).build();
        } catch (Throwable t) {
            log.error("Failed to remove replicas for tenant {} partition {}", new Object[] { tenantId, partitionId }, t);
            return Response.serverError().entity(t.getMessage()).build();
        }
    }

}
