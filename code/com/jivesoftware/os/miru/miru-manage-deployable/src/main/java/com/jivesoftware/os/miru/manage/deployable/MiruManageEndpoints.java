package com.jivesoftware.os.miru.manage.deployable;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import java.io.IOException;
import java.io.OutputStream;
import javax.ws.rs.Consumes;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

/**
 *
 */
@Path("/miru/manage")
public class MiruManageEndpoints {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruManageService miruManageService;
    private final MiruRebalanceDirector rebalanceDirector;

    public MiruManageEndpoints(@Context MiruManageService miruManageService, @Context MiruRebalanceDirector rebalanceDirector) {
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
    @Path("/lookup")
    @Produces(MediaType.TEXT_HTML)
    public Response getLookup() {
        String rendered = miruManageService.renderLookup();
        return Response.ok(rendered).build();
    }

    @GET
    @Path("/lookup/{tenantId}")
    @Produces(MediaType.TEXT_HTML)
    public Response getActivityWALForTenant(
        @PathParam("tenantId") String tenantId,
        @QueryParam("afterTimestamp") Long afterTimestamp,
        @QueryParam("limit") Integer limit) {
        String rendered = miruManageService.renderLookupWithFocus(
            new MiruTenantId(tenantId.getBytes(Charsets.UTF_8)),
            Optional.fromNullable(afterTimestamp),
            Optional.fromNullable(limit));
        return Response.ok(rendered).build();
    }

    @GET
    @Path("/wal/activity")
    @Produces(MediaType.TEXT_HTML)
    public Response getActivityWALForTenant() {
        String rendered = miruManageService.renderActivityWAL();
        return Response.ok(rendered).build();
    }

    @GET
    @Path("/wal/activity/{tenantId}")
    @Produces(MediaType.TEXT_HTML)
    public Response getActivityWALForTenant(@PathParam("tenantId") String tenantId) {
        String rendered = miruManageService.renderActivityWALWithTenant(new MiruTenantId(tenantId.getBytes(Charsets.UTF_8)));
        return Response.ok(rendered).build();
    }

    @GET
    @Path("/wal/activity/{tenantId}/{partitionId}")
    @Produces(MediaType.TEXT_HTML)
    public Response getActivityWALForTenantPartition(
        @PathParam("tenantId") String tenantId,
        @PathParam("partitionId") int partitionId,
        @QueryParam("sip") Boolean sip,
        @QueryParam("afterTimestamp") Long afterTimestamp,
        @QueryParam("limit") Integer limit) {
        String rendered = miruManageService.renderActivityWALWithFocus(
            new MiruTenantId(tenantId.getBytes(Charsets.UTF_8)),
            MiruPartitionId.of(partitionId),
            Optional.fromNullable(sip),
            Optional.fromNullable(afterTimestamp),
            Optional.fromNullable(limit));
        return Response.ok(rendered).build();
    }

    @GET
    @Path("/wal/read")
    @Produces(MediaType.TEXT_HTML)
    public Response getReadWALForTenant() {
        String rendered = miruManageService.renderReadWAL();
        return Response.ok(rendered).build();
    }

    @GET
    @Path("/wal/read/{tenantId}")
    @Produces(MediaType.TEXT_HTML)
    public Response getReadWALForTenant(@PathParam("tenantId") String tenantId) {
        String rendered = miruManageService.renderReadWALWithTenant(new MiruTenantId(tenantId.getBytes(Charsets.UTF_8)));
        return Response.ok(rendered).build();
    }

    @GET
    @Path("/wal/read/{tenantId}/{streamId}")
    @Produces(MediaType.TEXT_HTML)
    public Response getReadWALForTenantPartition(
        @PathParam("tenantId") String tenantId,
        @PathParam("streamId") String streamId,
        @QueryParam("sip") Boolean sip,
        @QueryParam("afterTimestamp") Long afterTimestamp,
        @QueryParam("limit") Integer limit) {
        String rendered = miruManageService.renderReadWALWithFocus(
            new MiruTenantId(tenantId.getBytes(Charsets.UTF_8)),
            streamId,
            Optional.fromNullable(sip),
            Optional.fromNullable(afterTimestamp),
            Optional.fromNullable(limit));
        return Response.ok(rendered).build();
    }

    @POST
    @Path("/topology/shift")
    @Produces(MediaType.TEXT_HTML)
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public Response shiftTopologies(@FormParam("host") String host, @FormParam("port") int port, @FormParam("direction") int direction) {
        try {
            ShiftPredicate shiftPredicate;
            if (direction == 0) {
                shiftPredicate = new UnhealthyTopologyShiftPredicate(0.24f); //TODO should be passed in
            } else {
                shiftPredicate = new RandomShiftPredicate(0.10f);
            }
            rebalanceDirector.shiftTopologies(new MiruHost(host, port),
                shiftPredicate,
                new CaterpillarSelectHostsStrategy(direction, false));
            return Response.ok("success").build();
        } catch (Throwable t) {
            LOG.error("/topology/shift {} {} {}", new Object[] { host, port, direction }, t);
            return Response.serverError().entity(t.getMessage()).build();
        }
    }

    @GET
    @Path("/topology/visual")
    @Produces("image/png")
    public Response visualizeTopologies(@QueryParam("width") final int width, @QueryParam("split") final int split, @QueryParam("index") final int index) {
        try {
            return Response.ok().entity(new StreamingOutput() {
                @Override
                public void write(OutputStream output)
                    throws IOException, WebApplicationException {
                    try {
                        rebalanceDirector.visualizeTopologies(width, split, index, output);
                        output.flush();
                    } catch (Exception e) {
                        throw new IOException("Problem generating visual", e);
                    }
                }
            }).build();
        } catch (Throwable t) {
            LOG.error("/topology/visual {}", new Object[] { width }, t);
            return Response.serverError().entity(t.getMessage()).build();
        }
    }
}
