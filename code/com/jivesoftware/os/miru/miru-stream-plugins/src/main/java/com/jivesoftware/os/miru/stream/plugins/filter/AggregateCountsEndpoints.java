package com.jivesoftware.os.miru.stream.plugins.filter;

import com.google.common.base.Optional;
import com.jivesoftware.os.jive.utils.jaxrs.util.ResponseHelper;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.query.MiruPartitionUnavailableException;
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
    public Response filterCustomStream(MiruAggregateCountsQueryParams params) {
        try {
            AggregateCountsResult result = injectable.filterCustomStream(
                    buildAggregateCountsQuery(params.getTenantId(), params.getQueryCriteria()));

            //log.info("filterCustomStream: " + result.collectedDistincts);
            return responseHelper.jsonResponse(result != null ? result : AggregateCountsResult.EMPTY_RESULTS);
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
    public Response filterInboxStreamAll(MiruAggregateCountsQueryParams params) {
        try {
            AggregateCountsResult result = injectable.filterInboxStreamAll(
                    buildAggregateCountsQuery(params.getTenantId(), params.getQueryCriteria()));

            //log.info("filterInboxStreamAll: " + result.collectedDistincts);
            return responseHelper.jsonResponse(result != null ? result : AggregateCountsResult.EMPTY_RESULTS);
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
    public Response filterInboxStreamUnread(MiruAggregateCountsQueryParams params) {
        try {
            AggregateCountsResult result = injectable.filterInboxStreamUnread(
                    buildAggregateCountsQuery(params.getTenantId(), params.getQueryCriteria()));

            //log.info("filterInboxStreamUnread: " + result.collectedDistincts);
            return responseHelper.jsonResponse(result != null ? result : AggregateCountsResult.EMPTY_RESULTS);
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
    public Response filterCustomStream(@PathParam("partitionId") int id, MiruAggregateCountsQueryAndResultParams params) {
        MiruPartitionId partitionId = MiruPartitionId.of(id);
        try {
            AggregateCountsResult result = injectable.filterCustomStream(partitionId, params.getQuery(), params.getLastResult());

            return responseHelper.jsonResponse(result != null ? result : AggregateCountsResult.EMPTY_RESULTS);
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
    public Response filterInboxStreamAll(@PathParam("partitionId") int id, MiruAggregateCountsQueryAndResultParams params) {
        MiruPartitionId partitionId = MiruPartitionId.of(id);
        try {
            AggregateCountsResult result = injectable.filterInboxStreamAll(partitionId, params.getQuery(), params.getLastResult());

            return responseHelper.jsonResponse(result != null ? result : AggregateCountsResult.EMPTY_RESULTS);
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
    public Response filterInboxStreamUnread(@PathParam("partitionId") int id, MiruAggregateCountsQueryAndResultParams params) {
        MiruPartitionId partitionId = MiruPartitionId.of(id);
        try {
            AggregateCountsResult result = injectable.filterInboxStreamUnread(partitionId, params.getQuery(), params.getLastResult());

            return responseHelper.jsonResponse(result != null ? result : AggregateCountsResult.EMPTY_RESULTS);
        } catch (MiruPartitionUnavailableException e) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Partition unavailable").build();
        } catch (Exception e) {
            log.error("Failed to filter inbox stream unread for partition: " + partitionId.getId(), e);
            return Response.serverError().build();
        }
    }

    private AggregateCountsQuery buildAggregateCountsQuery(MiruTenantId tenantId, MiruAggregateCountsQueryCriteria queryCriteria) {
        return new AggregateCountsQuery(
                tenantId,
                Optional.fromNullable(queryCriteria.getStreamId() != null ? new MiruStreamId(queryCriteria.getStreamId().toBytes()) : null),
                Optional.fromNullable(queryCriteria.getAnswerTimeRange()),
                Optional.fromNullable(queryCriteria.getCountTimeRange()),
                Optional.fromNullable(queryCriteria.getAuthzExpression()),
                queryCriteria.getStreamFilter(),
                Optional.fromNullable(queryCriteria.getConstraintsFilter()),
                queryCriteria.getQuery(),
                queryCriteria.getAggregateCountAroundField(),
                queryCriteria.getStartFromDistinctN(),
                queryCriteria.getDesiredNumberOfDistincts());
    }

}
