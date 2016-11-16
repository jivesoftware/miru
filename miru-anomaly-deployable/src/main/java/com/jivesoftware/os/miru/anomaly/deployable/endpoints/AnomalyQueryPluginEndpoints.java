package com.jivesoftware.os.miru.anomaly.deployable.endpoints;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.jivesoftware.os.miru.anomaly.deployable.MiruAnomalyService;
import com.jivesoftware.os.miru.anomaly.deployable.region.AnomalyQueryPluginRegion;
import com.jivesoftware.os.miru.anomaly.deployable.region.AnomalyQueryPluginRegion.AnomalyPluginRegionInput;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.List;
import java.util.Map;
import javax.inject.Singleton;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 *
 */
@Singleton
@Path("/ui/anomaly/query")
public class AnomalyQueryPluginEndpoints {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruAnomalyService anomalyService;
    private final AnomalyQueryPluginRegion pluginRegion;

    public AnomalyQueryPluginEndpoints(@Context MiruAnomalyService anomalyService, @Context AnomalyQueryPluginRegion pluginRegion) {
        this.anomalyService = anomalyService;
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
        @QueryParam("fromAgo") @DefaultValue("1") int fromAgo,
        @QueryParam("toAgo") @DefaultValue("0") int toAgo,
        @QueryParam("fromTimeUnit") @DefaultValue("HOURS") String fromTimeUnit,
        @QueryParam("toTimeUnit") @DefaultValue("HOURS") String toTimeUnit,
        @QueryParam("tenant") @DefaultValue("") String tenant,
        @QueryParam("sampler") @DefaultValue("") String sampler,
        @QueryParam("metric") @DefaultValue("") String metric,
        @QueryParam("tags") @DefaultValue("") String tags,
        @QueryParam("type") @DefaultValue("") String type,
        @QueryParam("buckets") @DefaultValue("30") int buckets,
        @QueryParam("graphType") @DefaultValue("Line") String graphType,
        @QueryParam("expansionField") @DefaultValue("metric") String expansionField,
        @QueryParam("expansionValue") @DefaultValue("") String expansionValue,
        @QueryParam("maxWaveforms") @DefaultValue("100") int maxWaveforms,
        @QueryParam("querySummary") @DefaultValue("false") boolean querySummary
    ) {
        try {
            String rendered = anomalyService.renderPlugin(pluginRegion,
                Optional.of(new AnomalyPluginRegionInput(cluster,
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
                    expansionValue,
                    maxWaveforms,
                    querySummary)));
            return Response.ok(rendered).build();
        } catch (Throwable t) {
            LOG.error("Failed query", t);
            return Response.serverError().build();
        }
    }

    @GET
    @Path("/typeahead/{fieldName}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response typeahead(
        @PathParam("fieldName") @DefaultValue("") String fieldName,
        @QueryParam("contains") @DefaultValue("") String contains) {
        try {
            List<Map<String, String>> data = pluginRegion.typeahead(fieldName, contains);
            return Response.ok(new ObjectMapper().writeValueAsString(data)).build();
        } catch (Exception x) {
            LOG.error("Failed to generating query ui.", x);
            return Response.serverError().build();
        }
    }
}
