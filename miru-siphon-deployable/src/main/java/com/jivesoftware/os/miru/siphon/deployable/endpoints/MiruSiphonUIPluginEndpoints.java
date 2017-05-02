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

    public MiruSiphonUIPluginEndpoints(@Context MiruSiphonUIService miruSiphonUIService,
        @Context MiruSiphonPluginRegion pluginRegion) {

        this.miruSiphonUIService = miruSiphonUIService;
        this.pluginRegion = pluginRegion;
    }

    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response query(
        @QueryParam("uniqueId") @DefaultValue("-1") long uniqueId,
        @QueryParam("name") @DefaultValue("") String name,
        @QueryParam("description") @DefaultValue("") String description,
        @QueryParam("siphonPluginName") @DefaultValue("") String siphonPluginName,
        @QueryParam("ringName") @DefaultValue("") String ringName,
        @QueryParam("partitionName") @DefaultValue("") String partitionName,
        @QueryParam("destinationTenantId") @DefaultValue("") String destinationTenantId,
        @QueryParam("batchSize") @DefaultValue("100") int batchSize,
        @QueryParam("action") @DefaultValue("") String action
    ) {

        try {
            String rendered = miruSiphonUIService.renderPlugin(pluginRegion,
                new MiruSiphonPluginRegionInput(uniqueId, name, description, siphonPluginName, ringName, partitionName, destinationTenantId, batchSize, action));
            return Response.ok(rendered).build();
        } catch (Exception x) {
            LOG.error("Failed to generating query ui.", x);
            return Response.serverError().build();
        }
    }

}
