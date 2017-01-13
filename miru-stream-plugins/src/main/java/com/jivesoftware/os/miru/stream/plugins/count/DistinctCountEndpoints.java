package com.jivesoftware.os.miru.stream.plugins.count;

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

import static com.jivesoftware.os.miru.stream.plugins.count.DistinctCountConstants.COUNT_PREFIX;
import static com.jivesoftware.os.miru.stream.plugins.count.DistinctCountConstants.CUSTOM_QUERY_ENDPOINT;
import static com.jivesoftware.os.miru.stream.plugins.count.DistinctCountConstants.INBOX_ALL_QUERY_ENDPOINT;

@Path(COUNT_PREFIX)
@Singleton
public class DistinctCountEndpoints {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final DistinctCountInjectable injectable;
    private final ResponseHelper responseHelper = ResponseHelper.INSTANCE;

    public DistinctCountEndpoints(@Context DistinctCountInjectable injectable) {
        this.injectable = injectable;
    }

    @POST
    @Path(CUSTOM_QUERY_ENDPOINT)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response countCustomStream(MiruRequest<DistinctCountQuery> request) {
        try {
            MiruResponse<DistinctCountAnswer> result = injectable.countCustomStream(request);

            //log.info("countCustomStream: " + answer.collectedDistincts);
            return responseHelper.jsonResponse(result);
        } catch (MiruPartitionUnavailableException | InterruptedException e) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Unavailable " + e.getMessage()).build();
        } catch (Exception e) {
            log.error("Failed to gather counts for custom stream.", e);
            return Response.serverError().build();
        }
    }

    @POST
    @Path(INBOX_ALL_QUERY_ENDPOINT)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response countInboxStreamAll(MiruRequest<DistinctCountQuery> request) {
        try {
            MiruResponse<DistinctCountAnswer> result = injectable.countInboxStream(request);

            //log.info("countInboxStreamAll: " + answer.collectedDistincts);
            return responseHelper.jsonResponse(result);
        } catch (MiruPartitionUnavailableException | InterruptedException e) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Unavailable " + e.getMessage()).build();
        } catch (Exception e) {
            log.error("Failed to gather counts for inbox.", e);
            return Response.serverError().build();
        }
    }

    @POST
    @Path(CUSTOM_QUERY_ENDPOINT + "/{partitionId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response countCustomStream(@PathParam("partitionId") int id, MiruRequestAndReport<DistinctCountQuery, DistinctCountReport> requestAndReport) {
        MiruPartitionId partitionId = MiruPartitionId.of(id);
        try {
            MiruPartitionResponse<DistinctCountAnswer> result = injectable.countCustomStream(partitionId, requestAndReport);

            return responseHelper.jsonResponse(result != null ? result : new MiruPartitionResponse<>(DistinctCountAnswer.EMPTY_RESULTS, null));
        } catch (MiruPartitionUnavailableException | InterruptedException e) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Unavailable " + e.getMessage()).build();
        } catch (Exception e) {
            log.error("Failed to gather counts for custom stream for partition: " + partitionId.getId(), e);
            return Response.serverError().build();
        }
    }

    @POST
    @Path(INBOX_ALL_QUERY_ENDPOINT + "/{partitionId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response countInboxStreamAll(@PathParam("partitionId") int id, MiruRequestAndReport<DistinctCountQuery, DistinctCountReport> requestAndReport) {
        MiruPartitionId partitionId = MiruPartitionId.of(id);
        try {
            MiruPartitionResponse<DistinctCountAnswer> result = injectable.countInboxStream(partitionId, requestAndReport);

            return responseHelper.jsonResponse(result != null ? result : new MiruPartitionResponse<>(DistinctCountAnswer.EMPTY_RESULTS, null));
        } catch (MiruPartitionUnavailableException | InterruptedException e) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Unavailable " + e.getMessage()).build();
        } catch (Exception e) {
            log.error("Failed to gather counts for inbox for partition: " + partitionId.getId(), e);
            return Response.serverError().build();
        }
    }
}
