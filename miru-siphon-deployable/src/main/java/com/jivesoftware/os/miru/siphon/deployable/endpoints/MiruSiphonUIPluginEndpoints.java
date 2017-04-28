package com.jivesoftware.os.miru.siphon.deployable.endpoints;

import com.jivesoftware.os.miru.siphon.deployable.MiruSiphonUIService;
import com.jivesoftware.os.miru.siphon.deployable.region.MiruSiphonPluginRegion;
import com.jivesoftware.os.miru.siphon.deployable.region.MiruSiphonPluginRegionInput;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import javax.inject.Singleton;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
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
@Path("/ui/siphon")
public class MiruSiphonUIPluginEndpoints {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruSiphonUIService miruSiphonUIService;
    private final MiruSiphonPluginRegion pluginRegion;

    public MiruSiphonUIPluginEndpoints(@Context MiruSiphonUIService miruSiphonUIService, @Context MiruSiphonPluginRegion pluginRegion) {
        this.miruSiphonUIService = miruSiphonUIService;
        this.pluginRegion = pluginRegion;
    }

    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response query(
        @QueryParam("tenantId") @DefaultValue("") String tenantId,
        @QueryParam("query") @DefaultValue("") String query,
        @QueryParam("folderGuids") @DefaultValue("") String folderGuids,
        @QueryParam("userGuids") @DefaultValue("") String userGuids,
        @QueryParam("querier") @DefaultValue("miru") String querier,
        @QueryParam("numberOfResult") @DefaultValue("100") int numberOfResult,
        @QueryParam("wildcardExpansion") @DefaultValue("false") boolean wildcardExpansion
    ) {

        try {
            String rendered = miruSiphonUIService.renderPlugin(pluginRegion, new MiruSiphonPluginRegionInput(tenantId, query, folderGuids, userGuids, querier,
                numberOfResult, wildcardExpansion));
            return Response.ok(rendered).build();
        } catch (Exception x) {
            LOG.error("Failed to generating query ui.", x);
            return Response.serverError().build();
        }
    }

}
