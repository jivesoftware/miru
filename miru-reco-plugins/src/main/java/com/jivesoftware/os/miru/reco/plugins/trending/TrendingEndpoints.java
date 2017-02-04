package com.jivesoftware.os.miru.reco.plugins.trending;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.miru.analytics.plugins.analytics.AnalyticsAnswer;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.plugin.partition.MiruPartitionUnavailableException;
import com.jivesoftware.os.miru.plugin.solution.MiruPartitionResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruRequestAndReport;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.shared.ResponseHelper;
import java.util.List;
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

import static com.jivesoftware.os.miru.reco.plugins.trending.TrendingConstants.CUSTOM_QUERY_ENDPOINT;
import static com.jivesoftware.os.miru.reco.plugins.trending.TrendingConstants.TRENDING_PREFIX;

@Singleton
@Path(TRENDING_PREFIX)
public class TrendingEndpoints {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();
    private static final FSTConfiguration conf = FSTConfiguration.createDefaultConfiguration();

    private final TrendingInjectable injectable;
    private final ObjectMapper objectMapper;
    private final JavaType resultType;
    private final ResponseHelper responseHelper = ResponseHelper.INSTANCE;

    public TrendingEndpoints(
        @Context TrendingInjectable injectable,
        @Context ObjectMapper objectMapper) {
        this.injectable = injectable;
        this.objectMapper = objectMapper;
        this.resultType = objectMapper.getTypeFactory().constructParametricType(MiruRequestAndReport.class, TrendingQuery.class, TrendingReport.class);
    }

    @POST
    @Path(CUSTOM_QUERY_ENDPOINT)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response scoreTrending(MiruRequest<TrendingQuery> request) {
        try {
            long t = System.currentTimeMillis();
            MiruResponse<TrendingAnswer> response = injectable.scoreTrending(request);

            if (log.isInfoEnabled()) {
                int resultsCount = 0;
                int desiredCount = 0;
                for (TrendingQueryScoreSet queryScoreSet : request.query.scoreSets) {
                    TrendingAnswerScoreSet answerScoreSet = response.answer.scoreSets.get(queryScoreSet.key);
                    if (answerScoreSet != null) {
                        for (List<Trendy> trendies : answerScoreSet.results.values()) {
                            resultsCount += trendies.size();
                        }
                    }
                    desiredCount += queryScoreSet.desiredNumberOfDistincts;
                }
                log.info("scoreTrending: {} / {} in {} ms", resultsCount, desiredCount, (System.currentTimeMillis() - t));
            }
            return responseHelper.jsonResponse(response);
        } catch (MiruPartitionUnavailableException | InterruptedException e) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Unavailable " + e.getMessage()).build();
        } catch (Exception e) {
            log.error("Failed to score trending.", e);
            return Response.serverError().build();
        }
    }

    @POST
    @Path(CUSTOM_QUERY_ENDPOINT + "/{partitionId}")
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response scoreTrending(@PathParam("partitionId") int id, byte[] rawBytes) {
        MiruPartitionId partitionId = MiruPartitionId.of(id);

        MiruRequestAndReport<TrendingQuery, TrendingReport> requestAndReport;
        try {
            requestAndReport = (MiruRequestAndReport<TrendingQuery, TrendingReport>) conf.asObject(rawBytes);
        } catch (Exception e) {
            log.error("Failed to deserialize request", e);
            return Response.serverError().build();
        }

        try {
            //byte[] jsonBytes = Snappy.uncompress(rawBytes);
            //MiruRequestAndReport<TrendingQuery, TrendingReport> requestAndReport = objectMapper.readValue(jsonBytes, resultType);
            MiruPartitionResponse<AnalyticsAnswer> result = injectable.scoreTrending(partitionId, requestAndReport);
            //byte[] responseBytes = result != null ? Snappy.compress(objectMapper.writeValueAsBytes(result)) : new byte[0];
            byte[] responseBytes = result != null ? conf.asByteArray(result) : new byte[0];
            return Response.ok(responseBytes, MediaType.APPLICATION_OCTET_STREAM).build();
        } catch (MiruPartitionUnavailableException | InterruptedException e) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Unavailable " + e.getMessage()).build();
        } catch (Exception e) {
            log.error("Failed to score trending for tenant: {} partition: {}", new Object[] { requestAndReport.request.tenantId, partitionId }, e);
            return Response.serverError().build();
        }
    }
}
