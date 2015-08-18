package com.jivesoftware.os.miru.sea.anomaly.deployable.endpoints;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.sea.anomaly.deployable.MiruSeaAnomalyService;
import com.jivesoftware.os.miru.sea.anomaly.deployable.region.SeaAnomalyQueryPluginRegion;
import com.jivesoftware.os.miru.sea.anomaly.deployable.region.SeaAnomalyQueryPluginRegion.SeaAnomalyPluginRegionInput;
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
@Path("/seaAnomaly/query")
public class SeaAnomalyQueryPluginEndpoints {

    private final MiruSeaAnomalyService seaAnomalyService;
    private final SeaAnomalyQueryPluginRegion pluginRegion;

    public SeaAnomalyQueryPluginEndpoints(@Context MiruSeaAnomalyService seaAnomalyService, @Context SeaAnomalyQueryPluginRegion pluginRegion) {
        this.seaAnomalyService = seaAnomalyService;
        this.pluginRegion = pluginRegion;
    }

    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response query(
        @QueryParam("cluster") @DefaultValue("") String cluster,
        @QueryParam("host") @DefaultValue("") String host,
        @QueryParam("service") @DefaultValue("") String service,
        @QueryParam("instance") @DefaultValue("") String instance,
        @QueryParam("version") @DefaultValue("") String version,
        @QueryParam("fromAgo") @DefaultValue("60") int fromAgo,
        @QueryParam("toAgo") @DefaultValue("0") int toAgo,
        @QueryParam("fromTimeUnit") @DefaultValue("MINUTES") String fromTimeUnit,
        @QueryParam("toTimeUnit") @DefaultValue("MINUTES") String toTimeUnit,
        @QueryParam("tenant") @DefaultValue("") String tenant,
        @QueryParam("sampler") @DefaultValue("") String sampler,
        @QueryParam("metric") @DefaultValue("") String metric,
        @QueryParam("tags") @DefaultValue("") String tags,
        @QueryParam("type") @DefaultValue("VALUE") String type,
        @QueryParam("buckets") @DefaultValue("30") int buckets,
        @QueryParam("graphType") @DefaultValue("Default") String graphType,
        @QueryParam("expansionField") @DefaultValue("metric") String expansionField,
        @QueryParam("expansionValue") @DefaultValue("*") String expansionValue
    ) {
        String rendered = seaAnomalyService.renderPlugin(pluginRegion,
            Optional.of(new SeaAnomalyPluginRegionInput(cluster,
                    host,
                    service,
                    instance,
                    version,
                    fromAgo,
                    toAgo,
                    fromTimeUnit,
                    toTimeUnit,
                    tenant,
                    sampler,
                    metric,
                    tags,
                    type,
                    buckets,
                    graphType,
                    expansionField,
                    expansionValue)));
        return Response.ok(rendered).build();
    }
}
