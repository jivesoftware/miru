package com.jivesoftware.os.miru.stream.plugins.count;

import com.jivesoftware.os.jive.utils.jaxrs.util.ResponseHelper;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.query.partition.MiruPartitionUnavailableException;
import com.jivesoftware.os.miru.query.solution.MiruResponse;
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
import static com.jivesoftware.os.miru.stream.plugins.count.DistinctCountConstants.INBOX_UNREAD_QUERY_ENDPOINT;

@Path(COUNT_PREFIX)
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
    public Response countCustomStream(DistinctCountQuery query) {
        try {
            MiruResponse<DistinctCountAnswer> result = injectable.countCustomStream(query);

            //log.info("countCustomStream: " + answer.collectedDistincts);
            return responseHelper.jsonResponse(result);
        } catch (MiruPartitionUnavailableException e) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Partition unavailable").build();
        } catch (Exception e) {
            log.error("Failed to gather counts for custom stream.", e);
            return Response.serverError().build();
        }
    }

    @POST
    @Path(INBOX_ALL_QUERY_ENDPOINT)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response countInboxStreamAll(DistinctCountQuery query) {
        try {
            MiruResponse<DistinctCountAnswer> result = injectable.countInboxStreamAll(query);

            //log.info("countInboxStreamAll: " + answer.collectedDistincts);
            return responseHelper.jsonResponse(result);
        } catch (MiruPartitionUnavailableException e) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Partition unavailable").build();
        } catch (Exception e) {
            log.error("Failed to gather counts for inbox.", e);
            return Response.serverError().build();
        }
    }

    @POST
    @Path(INBOX_UNREAD_QUERY_ENDPOINT)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response countInboxStreamUnread(DistinctCountQuery query) {
        try {
            MiruResponse<DistinctCountAnswer> result = injectable.countInboxStreamUnread(query);

            //log.info("countInboxStreamUnread: " + answer.collectedDistincts);
            return responseHelper.jsonResponse(result);
        } catch (MiruPartitionUnavailableException e) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Partition unavailable").build();
        } catch (Exception e) {
            log.error("Failed to gather counts for inbox (unread).", e);
            return Response.serverError().build();
        }
    }

    @POST
    @Path(CUSTOM_QUERY_ENDPOINT + "/{partitionId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response countCustomStream(@PathParam("partitionId") int id, DistinctCountQueryAndReport queryAndReport) {
        MiruPartitionId partitionId = MiruPartitionId.of(id);
        try {
            DistinctCountAnswer result = injectable.countCustomStream(partitionId, queryAndReport);

            return responseHelper.jsonResponse(result != null ? result : DistinctCountAnswer.EMPTY_RESULTS);
        } catch (MiruPartitionUnavailableException e) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Partition unavailable").build();
        } catch (Exception e) {
            log.error("Failed to gather counts for custom stream for partition: " + partitionId.getId(), e);
            return Response.serverError().build();
        }
    }

    @POST
    @Path(INBOX_ALL_QUERY_ENDPOINT + "/{partitionId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response countInboxStreamAll(@PathParam("partitionId") int id, DistinctCountQueryAndReport queryAndReport) {
        MiruPartitionId partitionId = MiruPartitionId.of(id);
        try {
            DistinctCountAnswer result = injectable.countInboxStreamAll(partitionId, queryAndReport);

            return responseHelper.jsonResponse(result != null ? result : DistinctCountAnswer.EMPTY_RESULTS);
        } catch (MiruPartitionUnavailableException e) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Partition unavailable").build();
        } catch (Exception e) {
            log.error("Failed to gather counts for inbox for partition: " + partitionId.getId(), e);
            return Response.serverError().build();
        }
    }

    @POST
    @Path(INBOX_UNREAD_QUERY_ENDPOINT + "/{partitionId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response countInboxStreamUnread(@PathParam("partitionId") int id, DistinctCountQueryAndReport queryAndReport) {
        MiruPartitionId partitionId = MiruPartitionId.of(id);
        try {
            DistinctCountAnswer result = injectable.countInboxStreamUnread(partitionId, queryAndReport);

            return responseHelper.jsonResponse(result != null ? result : DistinctCountAnswer.EMPTY_RESULTS);
        } catch (MiruPartitionUnavailableException e) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Partition unavailable").build();
        } catch (Exception e) {
            log.error("Failed to gather counts for inbox (unread) for partition: " + partitionId.getId(), e);
            return Response.serverError().build();
        }
    }
}
