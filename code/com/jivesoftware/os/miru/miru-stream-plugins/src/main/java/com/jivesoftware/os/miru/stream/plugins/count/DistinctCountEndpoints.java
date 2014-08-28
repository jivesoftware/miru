package com.jivesoftware.os.miru.stream.plugins.count;

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
    public Response countCustomStream(MiruDistinctCountQueryParams params) {
        try {
            DistinctCountResult result = injectable.countCustomStream(
                    buildDistinctCountQuery(params.getTenantId(), params.getQueryCriteria()));

            //log.info("countCustomStream: " + result.collectedDistincts);
            return responseHelper.jsonResponse(result != null ? result : DistinctCountResult.EMPTY_RESULTS);
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
    public Response countInboxStreamAll(MiruDistinctCountQueryParams params) {
        try {
            DistinctCountResult result = injectable.countInboxStreamAll(
                    buildDistinctCountQuery(params.getTenantId(), params.getQueryCriteria()));


            //log.info("countInboxStreamAll: " + result.collectedDistincts);
            return responseHelper.jsonResponse(result != null ? result : DistinctCountResult.EMPTY_RESULTS);
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
    public Response countInboxStreamUnread(MiruDistinctCountQueryParams params) {
        try {
            DistinctCountResult result = injectable.countInboxStreamUnread(
                    buildDistinctCountQuery(params.getTenantId(), params.getQueryCriteria()));


            //log.info("countInboxStreamUnread: " + result.collectedDistincts);
            return responseHelper.jsonResponse(result != null ? result : DistinctCountResult.EMPTY_RESULTS);
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
    public Response countCustomStream(@PathParam("partitionId") int id, MiruDistinctCountQueryAndResultParams params) {
        MiruPartitionId partitionId = MiruPartitionId.of(id);
        try {
            DistinctCountResult result = injectable.countCustomStream(partitionId, params.getQuery(), params.getLastResult());

            return responseHelper.jsonResponse(result != null ? result : DistinctCountResult.EMPTY_RESULTS);
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
    public Response countInboxStreamAll(@PathParam("partitionId") int id, MiruDistinctCountQueryAndResultParams params) {
        MiruPartitionId partitionId = MiruPartitionId.of(id);
        try {
            DistinctCountResult result = injectable.countInboxStreamAll(partitionId, params.getQuery(), params.getLastResult());

            return responseHelper.jsonResponse(result != null ? result : DistinctCountResult.EMPTY_RESULTS);
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
    public Response countInboxStreamUnread(@PathParam("partitionId") int id, MiruDistinctCountQueryAndResultParams params) {
        MiruPartitionId partitionId = MiruPartitionId.of(id);
        try {
            DistinctCountResult result = injectable.countInboxStreamUnread(partitionId, params.getQuery(), params.getLastResult());

            return responseHelper.jsonResponse(result != null ? result : DistinctCountResult.EMPTY_RESULTS);
        } catch (MiruPartitionUnavailableException e) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Partition unavailable").build();
        } catch (Exception e) {
            log.error("Failed to gather counts for inbox (unread) for partition: " + partitionId.getId(), e);
            return Response.serverError().build();
        }
    }

    private DistinctCountQuery buildDistinctCountQuery(MiruTenantId tenantId,
            MiruDistinctCountQueryCriteria queryCriteria) {
        return new DistinctCountQuery(
                tenantId,
                Optional.fromNullable(queryCriteria.getStreamId() != null ? new MiruStreamId(queryCriteria.getStreamId().toBytes()) : null),
                Optional.fromNullable(queryCriteria.getTimeRange()),
                Optional.fromNullable(queryCriteria.getAuthzExpression()),
                queryCriteria.getStreamFilter(),
                Optional.fromNullable(queryCriteria.getConstraintsFilter()),
                queryCriteria.getAggregateCountAroundField(),
                queryCriteria.getDesiredNumberOfDistincts());
    }
}
