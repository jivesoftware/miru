package com.jivesoftware.os.miru.analytics.plugins.analytics;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.nustaq.serialization.FSTConfiguration;

import static com.jivesoftware.os.miru.analytics.plugins.analytics.AnalyticsConstants.ANALYTICS_PREFIX;
import static com.jivesoftware.os.miru.analytics.plugins.analytics.AnalyticsConstants.CUSTOM_QUERY_ENDPOINT;

@Singleton
@Path(ANALYTICS_PREFIX)
public class AnalyticsEndpoints {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();
    private static final FSTConfiguration conf = FSTConfiguration.createDefaultConfiguration();

    private final AnalyticsInjectable injectable;
    private final ObjectMapper objectMapper;
    private final JavaType resultType;
    private final ResponseHelper responseHelper = ResponseHelper.INSTANCE;

    public AnalyticsEndpoints(
        @Context AnalyticsInjectable injectable,
        @Context ObjectMapper objectMapper) {
        this.injectable = injectable;
        this.objectMapper = objectMapper;
        this.resultType = objectMapper.getTypeFactory().constructParametricType(MiruRequestAndReport.class, AnalyticsQuery.class, AnalyticsReport.class);
    }

    @POST
    @Path(CUSTOM_QUERY_ENDPOINT)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response scoreAnalyticing(MiruRequest<AnalyticsQuery> request) {
        try {
            long t = System.currentTimeMillis();
            MiruResponse<AnalyticsAnswer> response = injectable.score(request);

            if (response.answer != null && response.answer.waveforms != null) {
                log.info("scoreAnalyticing: " + response.answer.waveforms.size()
                    + " in " + (System.currentTimeMillis() - t) + " ms");
            }
            return responseHelper.jsonResponse(response);
        } catch (MiruPartitionUnavailableException | InterruptedException e) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Unavailable " + e.getMessage()).build();
        } catch (Exception e) {
            log.error("Failed to score analytics.", e);
            return Response.serverError().build();
        }
    }

    @POST
    @Path(CUSTOM_QUERY_ENDPOINT + "/{partitionId}")
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response scoreAnalyticing(@PathParam("partitionId") int id, byte[] rawBytes) {
        MiruPartitionId partitionId = MiruPartitionId.of(id);
        try {
            //byte[] jsonBytes = Snappy.uncompress(rawBytes);
            //MiruRequestAndReport<AnalyticsQuery, AnalyticsReport> requestAndReport = objectMapper.readValue(jsonBytes, resultType);
            MiruRequestAndReport<AnalyticsQuery, AnalyticsReport> requestAndReport = (MiruRequestAndReport<AnalyticsQuery, AnalyticsReport>) conf.asObject(
                rawBytes);
            MiruPartitionResponse<AnalyticsAnswer> result = injectable.score(partitionId, requestAndReport);

            //byte[] responseBytes = result != null ? Snappy.compress(objectMapper.writeValueAsBytes(result)) : new byte[0];
            byte[] responseBytes = result != null ? conf.asByteArray(result) : new byte[0];
            return Response.ok(responseBytes, MediaType.APPLICATION_OCTET_STREAM).build();
        } catch (MiruPartitionUnavailableException | InterruptedException e) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Unavailable " + e.getMessage()).build();
        } catch (Exception e) {
            log.error("Failed to score analytics for partition: " + partitionId.getId(), e);
            return Response.serverError().build();
        }
    }
}
