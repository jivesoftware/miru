package com.jivesoftware.os.miru.analytics.plugins.analytics;

import com.jivesoftware.os.jive.utils.jaxrs.util.ResponseHelper;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.plugin.partition.MiruPartitionUnavailableException;
import com.jivesoftware.os.miru.plugin.solution.MiruPartitionResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruRequestAndReport;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static com.jivesoftware.os.miru.analytics.plugins.analytics.AnalyticsConstants.ANALYTICS_PREFIX;
import static com.jivesoftware.os.miru.analytics.plugins.analytics.AnalyticsConstants.CUSTOM_QUERY_ENDPOINT;

@Path(ANALYTICS_PREFIX)
public class AnalyticsEndpoints {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final AnalyticsInjectable injectable;
    private final ResponseHelper responseHelper = ResponseHelper.INSTANCE;

    public AnalyticsEndpoints(
        @Context AnalyticsInjectable injectable) {
        this.injectable = injectable;
    }

    @POST
    @Path(CUSTOM_QUERY_ENDPOINT)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response scoreAnalyticing(MiruRequest<AnalyticsQuery> request) {
        try {
            long t = System.currentTimeMillis();
            MiruResponse<AnalyticsAnswer> response = injectable.score(request);

            log.info("scoreAnalyticing: " + response.answer.waveform.waveform.length
                + " in " + (System.currentTimeMillis() - t) + " ms");
            return responseHelper.jsonResponse(response);
        } catch (MiruPartitionUnavailableException e) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Partition unavailable").build();
        } catch (Exception e) {
            log.error("Failed to score trending.", e);
            return Response.serverError().build();
        }
    }

    @POST
    @Path(CUSTOM_QUERY_ENDPOINT + "/{partitionId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response scoreAnalyticing(@PathParam("partitionId") int id, MiruRequestAndReport<AnalyticsQuery, AnalyticsReport> requestAndReport) {
        MiruPartitionId partitionId = MiruPartitionId.of(id);
        try {
            MiruPartitionResponse<AnalyticsAnswer> result = injectable.score(partitionId, requestAndReport);
            return responseHelper.jsonResponse(result != null ? result : new MiruPartitionResponse<>(AnalyticsAnswer.EMPTY_RESULTS, null));
        } catch (MiruPartitionUnavailableException e) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Partition unavailable").build();
        } catch (Exception e) {
            log.error("Failed to score trending for partition: " + partitionId.getId(), e);
            return Response.serverError().build();
        }
    }
}
