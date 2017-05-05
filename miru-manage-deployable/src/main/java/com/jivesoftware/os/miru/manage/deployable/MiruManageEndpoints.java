package com.jivesoftware.os.miru.manage.deployable;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.cluster.MiruRegistryClusterClient;
import com.jivesoftware.os.miru.manage.deployable.balancer.CaterpillarSelectHostsStrategy;
import com.jivesoftware.os.miru.manage.deployable.balancer.MiruRebalanceDirector;
import com.jivesoftware.os.miru.manage.deployable.balancer.ShiftPredicate;
import com.jivesoftware.os.miru.manage.deployable.balancer.UnhealthyTopologyShiftPredicate;
import com.jivesoftware.os.miru.manage.deployable.region.MiruSchemaRegion;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.InputStream;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;

/**
 *
 */
@Singleton
@Path("/ui")
public class MiruManageEndpoints {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruManageService miruManageService;
    private final MiruRebalanceDirector rebalanceDirector;
    private final MiruRegistryClusterClient registryClusterClient;

    public MiruManageEndpoints(@Context MiruManageService miruManageService,
        @Context MiruRebalanceDirector rebalanceDirector,
        @Context MiruRegistryClusterClient registryClusterClient) {
        this.miruManageService = miruManageService;
        this.rebalanceDirector = rebalanceDirector;
        this.registryClusterClient = registryClusterClient;
    }

    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response get(@HeaderParam("rb_session_redir_url") @DefaultValue("") String redirUrl) {
        String rendered = miruManageService.render(redirUrl);
        return Response.ok(rendered).build();
    }

    @GET
    @Path("/hosts")
    @Produces(MediaType.TEXT_HTML)
    public Response getHosts() {
        String rendered = miruManageService.renderHosts();
        return Response.ok(rendered).build();
    }

    @GET
    @Path("/hosts/{logicalName}")
    @Produces(MediaType.TEXT_HTML)
    public Response getHostsWithFocus(
        @PathParam("logicalName") String logicalName) {
        String rendered = miruManageService.renderHostsWithFocus(new MiruHost(logicalName));
        return Response.ok(rendered).build();
    }

    @GET
    @Path("/balancer")
    @Produces(MediaType.TEXT_HTML)
    public Response getBalancer() {
        String rendered = miruManageService.renderBalancer();
        return Response.ok(rendered).build();
    }

    @GET
    @Path("/balancer/export/{forceInstance}")
    @Produces(MediaType.TEXT_PLAIN)
    public Response importTopology(@PathParam("forceInstance") boolean forceInstance) {
        return Response.ok((StreamingOutput) output -> rebalanceDirector.exportTopology(output, forceInstance)).build();
    }

    @POST
    @Path("/balancer/import/{forceInstance}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces(MediaType.TEXT_HTML)
    public Response importTopology(@PathParam("forceInstance") boolean forceInstance,
        @FormDataParam("file") InputStream fileInputStream,
        @FormDataParam("file") FormDataContentDisposition contentDispositionHeader) {
        try {
            rebalanceDirector.importTopology(fileInputStream, forceInstance);
            return Response.ok("success").build();
        } catch (Throwable t) {
            return Response.serverError().entity(t.getMessage()).build();
        }
    }

    @GET
    @Path("/schema")
    @Produces(MediaType.TEXT_HTML)
    public Response getSchema() {
        String rendered = miruManageService.renderSchema(new MiruSchemaRegion.SchemaInput(null, null, -1, "lookup", false, false));
        return Response.ok(rendered).build();
    }

    @POST
    @Path("/schema")
    @Produces(MediaType.TEXT_HTML)
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public Response getSchemaWithLookup(@FormParam("tenantId") @DefaultValue("") String tenantId,
        @FormParam("lookupName") String lookupName,
        @FormParam("lookupVersion") @DefaultValue("-1") String lookupVersion,
        @FormParam("action") @DefaultValue("lookup") String action,
        @FormParam("upgradeOnMissing") @DefaultValue("false") boolean upgradeOnMissing,
        @FormParam("upgradeOnError") @DefaultValue("false") boolean upgradeOnError) {
        String rendered = miruManageService.renderSchema(new MiruSchemaRegion.SchemaInput(
            tenantId != null && !tenantId.trim().isEmpty() ? new MiruTenantId(tenantId.trim().getBytes(Charsets.UTF_8)) : null,
            lookupName != null && !lookupName.trim().isEmpty() ? lookupName.trim() : null,
            lookupVersion != null && !lookupVersion.trim().isEmpty() ? Integer.parseInt(lookupVersion.trim()) : -1,
            action, upgradeOnMissing, upgradeOnError));
        return Response.ok(rendered).build();
    }

    @GET
    @Path("/tenants")
    @Produces(MediaType.TEXT_HTML)
    public Response getTenants() {
        String rendered = miruManageService.renderTenants();
        return Response.ok(rendered).build();
    }

    @GET
    @Path("/tenants/{tenantId}")
    @Produces(MediaType.TEXT_HTML)
    public Response getTenantsForTenant(@PathParam("tenantId") String tenantId) {
        String rendered = miruManageService.renderTenantsWithFocus(new MiruTenantId(tenantId.getBytes(Charsets.UTF_8)));
        return Response.ok(rendered).build();
    }

    @GET
    @Path("/tenants/diff/{tenantId}/{partitionId}")
    @Produces(MediaType.TEXT_PLAIN)
    public Response getTenantsForTenant(@PathParam("tenantId") String tenantId,
        @PathParam("partitionId") int partitionId) {
        try {
            String rendered = rebalanceDirector.diffTenantPartition(new MiruTenantId(tenantId.getBytes(Charsets.UTF_8)), MiruPartitionId.of(partitionId));
            return Response.ok(rendered).build();
        } catch (Throwable t) {
            LOG.error("GET /tenants/diff/{}/{}", new Object[] { tenantId, partitionId }, t);
            return Response.serverError().entity(t.getMessage()).build();
        }
    }

    @POST
    @Path("/topology/shift")
    @Produces(MediaType.TEXT_HTML)
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public Response shiftTopologies(@FormParam("logicalName") String logicalName,
        @FormParam("direction") int direction,
        @FormParam("unhealthyPct") @DefaultValue("0.24") float unhealthyPct,
        @FormParam("probability") @DefaultValue("0.10") float probability) {
        try {
            ShiftPredicate shiftPredicate;
            boolean caterpillar;
            if (direction == 0) {
                // "zero" means evac
                caterpillar = false;
                shiftPredicate = new UnhealthyTopologyShiftPredicate(unhealthyPct); //TODO should be passed in
            } else {
                // "non-zero" means shift by N places
                caterpillar = true;
                shiftPredicate = new RandomShiftPredicate(probability);
            }
            rebalanceDirector.shiftTopologies(Optional.of(new MiruHost(logicalName)),
                shiftPredicate,
                new CaterpillarSelectHostsStrategy(caterpillar, direction, false));
            return Response.ok("success").build();
        } catch (Throwable t) {
            LOG.error("POST /topology/shift {} {} {} {} {}", new Object[] { logicalName, direction, unhealthyPct, probability }, t);
            return Response.serverError().entity(t.getMessage()).build();
        }
    }

    @POST
    @Path("/topology/repair")
    @Produces(MediaType.TEXT_HTML)
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public Response repairTopologies() {
        try {
            rebalanceDirector.shiftTopologies(Optional.<MiruHost>absent(),
                (tenantId, partitionId, hostHeartbeats, partitions) -> true,
                new CaterpillarSelectHostsStrategy(true, 0, false));
            return Response.ok("success").build();
        } catch (Throwable t) {
            LOG.error("POST /topology/repair", t);
            return Response.serverError().entity(t.getMessage()).build();
        }
    }

    @DELETE
    @Path("/hosts/{logicalName}")
    @Produces(MediaType.TEXT_HTML)
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public Response removeHost(@PathParam("logicalName") String logicalName) {
        try {
            registryClusterClient.removeHost(new MiruHost(logicalName));
            return Response.ok("success").build();
        } catch (Throwable t) {
            LOG.error("DELETE /hosts/{}", new Object[] { logicalName }, t);
            return Response.serverError().entity(t.getMessage()).build();
        }
    }

    @POST
    @Path("/tenants/rebuild")
    @Produces(MediaType.TEXT_HTML)
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public Response rebuildTenantPartition(@FormParam("logicalName") String logicalName,
        @FormParam("tenantId") String tenantId,
        @FormParam("partitionId") int partitionId) {
        try {
            rebalanceDirector.rebuildTenantPartition(new MiruHost(logicalName),
                new MiruTenantId(tenantId.getBytes(Charsets.UTF_8)),
                MiruPartitionId.of(partitionId));
            return Response.ok("success").build();
        } catch (Throwable t) {
            LOG.error("POST /tenants/rebuild {} {} {} {}", new Object[] { logicalName, tenantId, partitionId }, t);
            return Response.serverError().entity(t.getMessage()).build();
        }
    }

    @GET
    @Path("/topology/debugTenant/{tenantId}")
    @Produces(MediaType.TEXT_HTML)
    public Response debugTenant(@PathParam("tenantId") String tenantId) {
        try {
            StringBuilder stringBuilder = new StringBuilder();
            rebalanceDirector.debugTenant(new MiruTenantId(tenantId.getBytes(Charsets.UTF_8)), stringBuilder);
            return Response.ok(stringBuilder.toString()).build();
        } catch (Throwable t) {
            LOG.error("GET /topology/debugTenant/{}", new Object[] { tenantId }, t);
            return Response.serverError().entity(t.getMessage()).build();
        }
    }

    @GET
    @Path("/topology/debugTenantPartition/{tenantId}/{partitionId}")
    @Produces(MediaType.TEXT_HTML)
    public Response debugTenantPartition(@PathParam("tenantId") String tenantId,
        @PathParam("partitionId") int partitionId) {
        try {
            StringBuilder stringBuilder = new StringBuilder();
            rebalanceDirector.debugTenantPartition(new MiruTenantId(tenantId.getBytes(Charsets.UTF_8)), MiruPartitionId.of(partitionId), stringBuilder);
            return Response.ok(stringBuilder.toString()).build();
        } catch (Throwable t) {
            LOG.error("GET /topology/debugTenantPartition/{}/{}", new Object[] { tenantId, partitionId }, t);
            return Response.serverError().entity(t.getMessage()).build();
        }
    }
}
