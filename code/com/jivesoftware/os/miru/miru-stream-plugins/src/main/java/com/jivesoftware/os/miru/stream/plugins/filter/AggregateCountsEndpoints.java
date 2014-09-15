package com.jivesoftware.os.miru.stream.plugins.filter;

import com.jivesoftware.os.jive.utils.jaxrs.util.ResponseHelper;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.query.partition.MiruPartitionUnavailableException;
import com.jivesoftware.os.miru.query.solution.MiruPartitionResponse;
import com.jivesoftware.os.miru.query.solution.MiruRequest;
import com.jivesoftware.os.miru.query.solution.MiruRequestAndReport;
import com.jivesoftware.os.miru.query.solution.MiruResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static com.jivesoftware.os.miru.stream.plugins.filter.AggregateCountsConstants.CUSTOM_QUERY_ENDPOINT;
import static com.jivesoftware.os.miru.stream.plugins.filter.AggregateCountsConstants.FILTER_PREFIX;
import static com.jivesoftware.os.miru.stream.plugins.filter.AggregateCountsConstants.INBOX_ALL_QUERY_ENDPOINT;
import static com.jivesoftware.os.miru.stream.plugins.filter.AggregateCountsConstants.INBOX_UNREAD_QUERY_ENDPOINT;

@Path(FILTER_PREFIX)
public class AggregateCountsEndpoints {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final AggregateCountsInjectable injectable;
    private final ResponseHelper responseHelper = ResponseHelper.INSTANCE;

    public AggregateCountsEndpoints(@Context AggregateCountsInjectable injectable) {
        this.injectable = injectable;
    }

    @POST
    @Path(CUSTOM_QUERY_ENDPOINT)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response filterCustomStream(MiruRequest<AggregateCountsQuery> request) {
        try {
            MiruResponse<AggregateCountsAnswer> result = injectable.filterCustomStream(request);

            //log.info("filterCustomStream: " + answer.collectedDistincts);
            return responseHelper.jsonResponse(result);
        } catch (MiruPartitionUnavailableException e) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Partition unavailable").build();
        } catch (Exception e) {
            log.error("Failed to filter custom stream.", e);
            return Response.serverError().build();
        }
    }

    @POST
    @Path(INBOX_ALL_QUERY_ENDPOINT)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response filterInboxStreamAll(MiruRequest<AggregateCountsQuery> request) {
        try {
            MiruResponse<AggregateCountsAnswer> result = injectable.filterInboxStreamAll(request);

            //log.info("filterInboxStreamAll: " + answer.collectedDistincts);
            return responseHelper.jsonResponse(result);
        } catch (MiruPartitionUnavailableException e) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Partition unavailable").build();
        } catch (Exception e) {
            log.error("Failed to filter inbox.", e);
            return Response.serverError().build();
        }
    }

    @POST
    @Path(INBOX_UNREAD_QUERY_ENDPOINT)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response filterInboxStreamUnread(MiruRequest<AggregateCountsQuery> request) {
        try {
            MiruResponse<AggregateCountsAnswer> result = injectable.filterInboxStreamUnread(request);

            //log.info("filterInboxStreamUnread: " + answer.collectedDistincts);
            return responseHelper.jsonResponse(result);
        } catch (MiruPartitionUnavailableException e) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Partition unavailable").build();
        } catch (Exception e) {
            log.error("Failed to filter inbox (unread).", e);
            return Response.serverError().build();
        }
    }

    @POST
    @Path(CUSTOM_QUERY_ENDPOINT + "/{partitionId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response filterCustomStream(@PathParam("partitionId") int id, MiruRequestAndReport<AggregateCountsQuery, AggregateCountsReport> requestAndReport) {
        MiruPartitionId partitionId = MiruPartitionId.of(id);
        try {
            MiruPartitionResponse<AggregateCountsAnswer> result = injectable.filterCustomStream(partitionId, requestAndReport);

            return responseHelper.jsonResponse(result != null ? result : new MiruPartitionResponse<>(AggregateCountsAnswer.EMPTY_RESULTS, null));
        } catch (MiruPartitionUnavailableException e) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Partition unavailable").build();
        } catch (Exception e) {
            log.error("Failed to filter custom stream for partition: " + partitionId.getId(), e);
            return Response.serverError().build();
        }
    }

    @POST
    @Path(INBOX_ALL_QUERY_ENDPOINT + "/{partitionId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response filterInboxStreamAll(@PathParam("partitionId") int id, MiruRequestAndReport<AggregateCountsQuery, AggregateCountsReport> requestAndReport) {
        MiruPartitionId partitionId = MiruPartitionId.of(id);
        try {
            MiruPartitionResponse<AggregateCountsAnswer> result = injectable.filterInboxStreamAll(partitionId, requestAndReport);

            return responseHelper.jsonResponse(result != null ? result : new MiruPartitionResponse<>(AggregateCountsAnswer.EMPTY_RESULTS, null));
        } catch (MiruPartitionUnavailableException e) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Partition unavailable").build();
        } catch (Exception e) {
            log.error("Failed to filter inbox stream all for partition: " + partitionId.getId(), e);
            return Response.serverError().build();
        }
    }

    @POST
    @Path(INBOX_UNREAD_QUERY_ENDPOINT + "/{partitionId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response filterInboxStreamUnread(@PathParam("partitionId") int id, MiruRequestAndReport<AggregateCountsQuery, AggregateCountsReport> requestAndReport) {
        MiruPartitionId partitionId = MiruPartitionId.of(id);
        try {
            MiruPartitionResponse<AggregateCountsAnswer> result = injectable.filterInboxStreamUnread(partitionId, requestAndReport);

            return responseHelper.jsonResponse(result != null ? result : new MiruPartitionResponse<>(AggregateCountsAnswer.EMPTY_RESULTS, null));
        } catch (MiruPartitionUnavailableException e) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Partition unavailable").build();
        } catch (Exception e) {
            log.error("Failed to filter inbox stream unread for partition: " + partitionId.getId(), e);
            return Response.serverError().build();
        }
    }
}
