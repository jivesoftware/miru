package com.jivesoftware.os.miru.analytics.plugins.metrics;

import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.plugin.partition.MiruPartitionUnavailableException;
import com.jivesoftware.os.miru.plugin.solution.MiruPartitionResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruRequestAndReport;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.shared.ResponseHelper;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static com.jivesoftware.os.miru.analytics.plugins.metrics.MetricsConstants.CUSTOM_QUERY_ENDPOINT;
import static com.jivesoftware.os.miru.analytics.plugins.metrics.MetricsConstants.METRICS_PREFIX;

@Singleton
@Path(METRICS_PREFIX)
public class MetricsEndpoints {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final MetricsInjectable injectable;
    private final ResponseHelper responseHelper = ResponseHelper.INSTANCE;

    public MetricsEndpoints(
        @Context MetricsInjectable injectable) {
        this.injectable = injectable;
    }

    @POST
    @Path(CUSTOM_QUERY_ENDPOINT)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response scoreMetricing(MiruRequest<MetricsQuery> request) {
        try {
            long t = System.currentTimeMillis();
            MiruResponse<MetricsAnswer> response = injectable.score(request);

            if (response.answer != null && response.answer.waveforms != null) {
                log.info("scoring: " + response.answer.waveforms.size()
                    + " in " + (System.currentTimeMillis() - t) + " ms");
            }
            return responseHelper.jsonResponse(response);
        } catch (MiruPartitionUnavailableException | InterruptedException e) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Unavailable " + e.getMessage()).build();
        } catch (Exception e) {
            log.error("Failed to score.", e);
            return Response.serverError().build();
        }
    }

    @POST
    @Path(CUSTOM_QUERY_ENDPOINT + "/{partitionId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response scoreMetricing(@PathParam("partitionId") int id, MiruRequestAndReport<MetricsQuery, MetricsReport> requestAndReport) {
        MiruPartitionId partitionId = MiruPartitionId.of(id);
        try {
            MiruPartitionResponse<MetricsAnswer> result = injectable.score(partitionId, requestAndReport);
            return responseHelper.jsonResponse(result != null ? result : new MiruPartitionResponse<>(MetricsAnswer.EMPTY_RESULTS, null));
        } catch (MiruPartitionUnavailableException | InterruptedException e) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Unavailable " + e.getMessage()).build();
        } catch (Exception e) {
            log.error("Failed to score trending for partition: " + partitionId.getId(), e);
            return Response.serverError().build();
        }
    }
}
