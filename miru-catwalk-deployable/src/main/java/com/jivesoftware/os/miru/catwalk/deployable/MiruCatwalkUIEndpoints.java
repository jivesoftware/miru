package com.jivesoftware.os.miru.catwalk.deployable;

import com.jivesoftware.os.miru.catwalk.deployable.region.MiruInspectRegion.InspectInput;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import javax.inject.Singleton;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 *
 */
@Singleton
@Path("/ui")
public class MiruCatwalkUIEndpoints {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruCatwalkUIService miruCatwalkUIService;

    public MiruCatwalkUIEndpoints(@Context MiruCatwalkUIService miruCatwalkUIService) {
        this.miruCatwalkUIService = miruCatwalkUIService;
    }

    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response get(@HeaderParam("rb_session_redir_url") @DefaultValue("") String redirUrl) {
        String rendered = miruCatwalkUIService.render(redirUrl);
        return Response.ok(rendered).build();
    }

    @GET
    @Path("/inspect")
    @Produces(MediaType.TEXT_HTML)
    public Response getInspect(@QueryParam("tenantId") String tenantId,
        @QueryParam("catwalkId") String catwalkId,
        @QueryParam("modelId") String modelId,
        @QueryParam("features") String features) {
        try {
            String rendered = miruCatwalkUIService.renderInspect(new InspectInput(tenantId, catwalkId, modelId, features));
            return Response.ok(rendered).build();
        } catch (Exception e) {
            LOG.error("Failed to inspect {} {} {} {}", new Object[] { tenantId, catwalkId, modelId, features }, e);
            return Response.serverError().entity("Failed to inspect").build();
        }
    }

}
