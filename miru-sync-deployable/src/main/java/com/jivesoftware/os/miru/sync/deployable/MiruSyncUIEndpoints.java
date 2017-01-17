package com.jivesoftware.os.miru.sync.deployable;

import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.sync.deployable.region.MiruStatusRegionInput;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.nio.charset.StandardCharsets;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 *
 */
@Singleton
@Path("/ui")
public class MiruSyncUIEndpoints {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruSyncUIService syncUIService;

    public MiruSyncUIEndpoints(@Context MiruSyncUIService syncUIService) {
        this.syncUIService = syncUIService;
    }

    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response get() {
        String rendered = syncUIService.render();
        return Response.ok(rendered).build();
    }

    @GET
    @Path("/status")
    @Produces(MediaType.TEXT_HTML)
    public Response getStatus() {
        try {
            String rendered = syncUIService.renderStatus(null);
            return Response.ok(rendered).build();
        } catch (Throwable t) {
            LOG.error("Failed to getStatus", t);
            return Response.serverError().build();
        }
    }

    @GET
    @Path("/status/{syncspaceName}/{tenantId}")
    @Produces(MediaType.TEXT_HTML)
    public Response getStatus(@PathParam("syncspaceName") String syncspaceName,
        @PathParam("tenantId") String tenantId) {
        try {
            String rendered = syncUIService.renderStatus(new MiruStatusRegionInput(syncspaceName, new MiruTenantId(tenantId.getBytes(StandardCharsets.UTF_8))));
            return Response.ok(rendered).build();
        } catch (Throwable t) {
            LOG.error("Failed to getStatus({}, {})", new Object[] { syncspaceName, tenantId }, t);
            return Response.serverError().build();
        }
    }

}
