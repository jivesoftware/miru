package com.jivesoftware.os.miru.reader.deployable;

import com.google.common.base.Charsets;
import com.google.common.collect.ListMultimap;
import com.jivesoftware.os.jive.utils.jaxrs.util.ResponseHelper;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartition;
import com.jivesoftware.os.miru.api.MiruPartitionCoordInfo;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.config.PartitionsForTenantResult;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.service.MiruService;
import com.jivesoftware.os.miru.service.schema.RegistrySchemaProvider;
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

import static com.jivesoftware.os.miru.api.MiruConfigReader.CONFIG_SERVICE_ENDPOINT_PREFIX;
import static com.jivesoftware.os.miru.api.MiruConfigReader.PARTITIONS_ENDPOINT;

@Path(CONFIG_SERVICE_ENDPOINT_PREFIX)
public class MiruReaderConfigEndpoints {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final MiruClusterRegistry clusterRegistry;
    private final MiruService miruService;
    private final RegistrySchemaProvider registrySchemaProvider;
    private final ResponseHelper responseHelper = ResponseHelper.INSTANCE;

    public MiruReaderConfigEndpoints(@Context MiruClusterRegistry clusterRegistry,
        @Context MiruService miruService,
        @Context RegistrySchemaProvider registrySchemaProvider) {
        this.clusterRegistry = clusterRegistry;
        this.miruService = miruService;
        this.registrySchemaProvider = registrySchemaProvider;
    }

    @GET
    @Path(PARTITIONS_ENDPOINT + "/{tenantId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getPartitionsForTenant(@PathParam("tenantId") String tenantIdString) {
        try {
            MiruTenantId tenantId = new MiruTenantId(tenantIdString.getBytes(Charsets.UTF_8));
            ListMultimap<MiruPartitionState, MiruPartition> partitionsForTenant = clusterRegistry.getPartitionsForTenant(tenantId);
            PartitionsForTenantResult result = new PartitionsForTenantResult(partitionsForTenant.asMap());
            return Response.ok(result).build();
        } catch (Exception e) {
            log.error("Failed to filter custom stream.", e);
            return Response.serverError().build();
        }
    }

    @POST
    @Path("/schema/{tenantId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response setStorage(
        @PathParam("tenantId") String tenantId,
        MiruSchema schema) {
        try {
            registrySchemaProvider.register(
                new MiruTenantId(tenantId.getBytes(Charsets.UTF_8)),
                schema,
                //TODO client specified version?
                System.currentTimeMillis());
            return responseHelper.jsonResponse("Success");
        } catch (Throwable t) {
            log.error("Failed to set schema for tenant {}", new Object[] { tenantId }, t);
            return Response.serverError().entity(t.getMessage()).build();
        }
    }

    @GET
    @Path("/schema/{tenantId}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response setStorage(@PathParam("tenantId") String tenantId) {
        try {
            MiruSchema schema = registrySchemaProvider.getSchema(new MiruTenantId(tenantId.getBytes(Charsets.UTF_8)));
            return Response.ok(schema).build();
        } catch (Throwable t) {
            log.error("Failed to get schema for tenant {}", new Object[] { tenantId }, t);
            return Response.serverError().entity(t.getMessage()).build();
        }
    }

    @POST
    @Path("/storage/{tenantId}/{partitionId}/{storage}")
    @Produces(MediaType.TEXT_HTML)
    public Response setStorage(
        @PathParam("tenantId") String tenantId,
        @PathParam("partitionId") Integer partitionId,
        @PathParam("storage") String storage) {
        try {
            miruService.setStorage(
                new MiruTenantId(tenantId.getBytes(Charsets.UTF_8)),
                MiruPartitionId.of(partitionId),
                MiruBackingStorage.valueOf(storage));
            return Response.ok(storage).build();
        } catch (Throwable t) {
            log.error("Failed to set storage to {} for tenant {} partition {}", new Object[] { storage, tenantId, partitionId }, t);
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
            miruService.removeHost(host);
            return Response.ok(host.toStringForm()).build();
        } catch (Throwable t) {
            log.error("Failed to remove host {}", new Object[] { host }, t);
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
            miruService.removeTopology(
                new MiruTenantId(tenantId.getBytes(Charsets.UTF_8)),
                MiruPartitionId.of(partitionId),
                host);
            return Response.ok(host.toStringForm()).build();
        } catch (Throwable t) {
            log.error("Failed to remove topology for tenant {} partition {} host {}", new Object[] { tenantId, partitionId, host }, t);
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
            MiruTenantId tenant = new MiruTenantId(tenantId.getBytes(Charsets.UTF_8));
            MiruPartitionId partition = MiruPartitionId.of(partitionId);
            if (miruService.checkInfo(tenant, partition, new MiruPartitionCoordInfo(state, storage))) {
                return Response.noContent().build();
            } else {
                return Response.status(Response.Status.NOT_FOUND).build();
            }
        } catch (Throwable t) {
            log.error("Failed to check state for tenant {} partition {}", new Object[] { tenantId, partitionId });
            return Response.serverError().build();
        }
    }
}
