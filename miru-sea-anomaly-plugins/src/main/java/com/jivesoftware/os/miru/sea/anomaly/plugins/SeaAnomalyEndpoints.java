package com.jivesoftware.os.miru.sea.anomaly.plugins;

import com.jivesoftware.os.jive.utils.jaxrs.util.ResponseHelper;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.plugin.partition.MiruPartitionUnavailableException;
import com.jivesoftware.os.miru.plugin.solution.MiruPartitionResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruRequestAndReport;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static com.jivesoftware.os.miru.sea.anomaly.plugins.SeaAnomalyConstants.CUSTOM_QUERY_ENDPOINT;
import static com.jivesoftware.os.miru.sea.anomaly.plugins.SeaAnomalyConstants.SEA_ANOMALY_PREFIX;

@Path(SEA_ANOMALY_PREFIX)
public class SeaAnomalyEndpoints {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final SeaAnomalyInjectable injectable;
    private final ResponseHelper responseHelper = ResponseHelper.INSTANCE;

    public SeaAnomalyEndpoints(
        @Context SeaAnomalyInjectable injectable) {
        this.injectable = injectable;
    }

    @POST
    @Path(CUSTOM_QUERY_ENDPOINT)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response scoreStumptowning(MiruRequest<SeaAnomalyQuery> request) {
        try {
            long t = System.currentTimeMillis();
            MiruResponse<SeaAnomalyAnswer> response = injectable.score(request);

            if (response.answer != null && response.answer.waveforms != null) {
                log.info("scoreStumptowning: " + response.answer.waveforms.size()
                    + " in " + (System.currentTimeMillis() - t) + " ms");
            }
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
    public Response scoreStumptowning(@PathParam("partitionId") int id, MiruRequestAndReport<SeaAnomalyQuery, SeaAnomalyReport> requestAndReport) {
        MiruPartitionId partitionId = MiruPartitionId.of(id);
        try {
            MiruPartitionResponse<SeaAnomalyAnswer> result = injectable.score(partitionId, requestAndReport);
            return responseHelper.jsonResponse(result != null ? result : new MiruPartitionResponse<>(SeaAnomalyAnswer.EMPTY_RESULTS, null));
        } catch (MiruPartitionUnavailableException e) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Partition unavailable").build();
        } catch (Exception e) {
            log.error("Failed to score trending for partition: " + partitionId.getId(), e);
            return Response.serverError().build();
        }
    }
}
