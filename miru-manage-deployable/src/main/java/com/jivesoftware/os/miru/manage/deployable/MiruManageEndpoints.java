package com.jivesoftware.os.miru.manage.deployable;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.manage.deployable.balancer.CaterpillarSelectHostsStrategy;
import com.jivesoftware.os.miru.manage.deployable.balancer.MiruRebalanceDirector;
import com.jivesoftware.os.miru.manage.deployable.balancer.ShiftPredicate;
import com.jivesoftware.os.miru.manage.deployable.balancer.UnhealthyTopologyShiftPredicate;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.IOException;
import java.io.InputStream;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
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
@Path("/miru/manage")
public class MiruManageEndpoints {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruManageService miruManageService;
    private final MiruRebalanceDirector rebalanceDirector;

    public MiruManageEndpoints(@Context MiruManageService miruManageService,
        @Context MiruRebalanceDirector rebalanceDirector) {
        this.miruManageService = miruManageService;
        this.rebalanceDirector = rebalanceDirector;
    }

    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response get() {
        String rendered = miruManageService.render();
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
    @Path("/hosts/{logicalName}/{port}")
    @Produces(MediaType.TEXT_HTML)
    public Response getHostsWithFocus(
        @PathParam("logicalName") String logicalName,
        @PathParam("port") int port) {
        String rendered = miruManageService.renderHostsWithFocus(new MiruHost(logicalName, port));
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
    @Path("/balancer/export")
    @Produces(MediaType.TEXT_PLAIN)
    public Response importTopology() {
        StreamingOutput streamingOutput = rebalanceDirector::exportTopology;
        return Response.ok(streamingOutput).build();
    }

    @POST
    @Path("/balancer/import")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces(MediaType.TEXT_HTML)
    public Response importTopology(@FormDataParam("file") InputStream fileInputStream,
        @FormDataParam("file") FormDataContentDisposition contentDispositionHeader) {
        try {
            rebalanceDirector.importTopology(fileInputStream);
            return Response.ok("success").build();
        } catch (Throwable t) {
            return Response.serverError().entity(t.getMessage()).build();
        }
    }

    @GET
    @Path("/schema")
    @Produces(MediaType.TEXT_HTML)
    public Response getSchema() {
        String rendered = miruManageService.renderSchema();
        return Response.ok(rendered).build();
    }

    @POST
    @Path("/schema")
    @Produces(MediaType.TEXT_HTML)
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public Response getSchemaWithLookup(@FormParam("lookupJSON") @DefaultValue("") String lookupJSON) {
        String rendered;
        if (lookupJSON.trim().isEmpty()) {
            rendered = miruManageService.renderSchema();
        } else {
            rendered = miruManageService.renderSchemaWithLookup(lookupJSON);
        }
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

    @POST
    @Path("/topology/shift")
    @Produces(MediaType.TEXT_HTML)
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public Response shiftTopologies(@FormParam("host") String host,
        @FormParam("port") int port,
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
            rebalanceDirector.shiftTopologies(Optional.of(new MiruHost(host, port)),
                shiftPredicate,
                new CaterpillarSelectHostsStrategy(caterpillar, direction, false));
            return Response.ok("success").build();
        } catch (Throwable t) {
            LOG.error("POST /topology/shift {} {} {} {} {}", new Object[] { host, port, direction, unhealthyPct, probability }, t);
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

    @GET
    @Path("/topology/visual")
    @Produces("image/png")
    public Response visualizeTopologies(@QueryParam("width") final int width,
        @QueryParam("split") final int split,
        @QueryParam("index") final int index,
        @QueryParam("token") final String token) {
        try {
            return Response.ok().entity((StreamingOutput) output -> {
                try {
                    rebalanceDirector.visualizeTopologies(width, split, index, token, output);
                    output.flush();
                } catch (Exception e) {
                    throw new IOException("Problem generating visual", e);
                }
            }).build();
        } catch (Throwable t) {
            LOG.error("GET /topology/visual {}", new Object[] { width }, t);
            return Response.serverError().entity(t.getMessage()).build();
        }
    }

    @DELETE
    @Path("/hosts/{logicalName}/{port}")
    @Produces(MediaType.TEXT_HTML)
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public Response removeHost(@PathParam("logicalName") String logicalName, @PathParam("port") int port) {
        try {
            rebalanceDirector.removeHost(new MiruHost(logicalName, port));
            return Response.ok("success").build();
        } catch (Throwable t) {
            LOG.error("DELETE /hosts/{}/{}", new Object[] { logicalName, port }, t);
            return Response.serverError().entity(t.getMessage()).build();
        }
    }

    @POST
    @Path("/tenants/rebuild")
    @Produces(MediaType.TEXT_HTML)
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public Response rebuildTenantPartition(@FormParam("host") String host,
        @FormParam("port") int port,
        @FormParam("tenantId") String tenantId,
        @FormParam("partitionId") int partitionId) {
        try {
            rebalanceDirector.rebuildTenantPartition(new MiruHost(host, port),
                new MiruTenantId(tenantId.getBytes(Charsets.UTF_8)),
                MiruPartitionId.of(partitionId));
            return Response.ok("success").build();
        } catch (Throwable t) {
            LOG.error("POST /tenants/rebuild {} {} {} {}", new Object[] { host, port, tenantId, partitionId }, t);
            return Response.serverError().entity(t.getMessage()).build();
        }
    }
}
