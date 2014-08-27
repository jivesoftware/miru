package com.jivesoftware.os.miru.service.endpoint;

import com.jivesoftware.os.jive.utils.jaxrs.util.ResponseHelper;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.MiruAggregateCountsQueryAndResultParams;
import com.jivesoftware.os.miru.api.MiruAggregateCountsQueryParams;
import com.jivesoftware.os.miru.api.MiruDistinctCountQueryAndResultParams;
import com.jivesoftware.os.miru.api.MiruDistinctCountQueryParams;
import com.jivesoftware.os.miru.api.MiruReader;
import com.jivesoftware.os.miru.api.MiruTrendingQueryAndResultParams;
import com.jivesoftware.os.miru.api.MiruTrendingQueryParams;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.result.AggregateCountsResult;
import com.jivesoftware.os.miru.api.query.result.DistinctCountResult;
import com.jivesoftware.os.miru.api.query.result.TrendingResult;
import com.jivesoftware.os.miru.service.partition.MiruPartitionUnavailableException;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static com.jivesoftware.os.miru.api.MiruReader.AGGREGATE_COUNTS_INFIX;
import static com.jivesoftware.os.miru.api.MiruReader.CUSTOM_QUERY_ENDPOINT;
import static com.jivesoftware.os.miru.api.MiruReader.DISTINCT_COUNT_INFIX;
import static com.jivesoftware.os.miru.api.MiruReader.INBOX_ALL_QUERY_ENDPOINT;
import static com.jivesoftware.os.miru.api.MiruReader.INBOX_UNREAD_QUERY_ENDPOINT;
import static com.jivesoftware.os.miru.api.MiruReader.QUERY_SERVICE_ENDPOINT_PREFIX;
import static com.jivesoftware.os.miru.api.MiruReader.TRENDING_INFIX;
import static com.jivesoftware.os.miru.api.MiruReader.WARM_ENDPOINT;

@Path(QUERY_SERVICE_ENDPOINT_PREFIX)
public class MiruReaderEndpoints {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final MiruReader miruReader;
    private final ResponseHelper responseHelper = ResponseHelper.INSTANCE;

    public MiruReaderEndpoints(@Context MiruReader miruReader) {
        this.miruReader = miruReader;
    }

    @POST
    @Path(AGGREGATE_COUNTS_INFIX + CUSTOM_QUERY_ENDPOINT)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response filterCustomStream(MiruAggregateCountsQueryParams params) {
        try {
            AggregateCountsResult result = miruReader.filterCustomStream(
                params.getTenantId(),
                params.getUserIdentity(),
                params.getAuthzExpression(),
                params.getQueryCriteria());

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
    @Path(AGGREGATE_COUNTS_INFIX + INBOX_ALL_QUERY_ENDPOINT)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response filterInboxStreamAll(MiruAggregateCountsQueryParams params) {
        try {
            AggregateCountsResult result = miruReader.filterInboxStreamAll(
                params.getTenantId(),
                params.getUserIdentity(),
                params.getAuthzExpression(),
                params.getQueryCriteria());

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
    @Path(AGGREGATE_COUNTS_INFIX + INBOX_UNREAD_QUERY_ENDPOINT)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response filterInboxStreamUnread(MiruAggregateCountsQueryParams params) {
        try {
            AggregateCountsResult result = miruReader.filterInboxStreamUnread(
                params.getTenantId(),
                params.getUserIdentity(),
                params.getAuthzExpression(),
                params.getQueryCriteria());

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
    @Path(AGGREGATE_COUNTS_INFIX + CUSTOM_QUERY_ENDPOINT + "/{partitionId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response filterCustomStream(@PathParam("partitionId") int id, MiruAggregateCountsQueryAndResultParams params) {
        MiruPartitionId partitionId = MiruPartitionId.of(id);
        try {
            AggregateCountsResult result = miruReader.filterCustomStream(partitionId, params.getQuery(), params.getLastResult());

            return responseHelper.jsonResponse(result != null ? result : AggregateCountsResult.EMPTY_RESULTS);
        } catch (MiruPartitionUnavailableException e) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Partition unavailable").build();
        } catch (Exception e) {
            log.error("Failed to filter custom stream for partition: " + partitionId.getId(), e);
            return Response.serverError().build();
        }
    }

    @POST
    @Path(AGGREGATE_COUNTS_INFIX + INBOX_ALL_QUERY_ENDPOINT + "/{partitionId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response filterInboxStreamAll(@PathParam("partitionId") int id, MiruAggregateCountsQueryAndResultParams params) {
        MiruPartitionId partitionId = MiruPartitionId.of(id);
        try {
            AggregateCountsResult result = miruReader.filterInboxStreamAll(partitionId, params.getQuery(), params.getLastResult());

            return responseHelper.jsonResponse(result != null ? result : AggregateCountsResult.EMPTY_RESULTS);
        } catch (MiruPartitionUnavailableException e) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Partition unavailable").build();
        } catch (Exception e) {
            log.error("Failed to filter inbox stream all for partition: " + partitionId.getId(), e);
            return Response.serverError().build();
        }
    }

    @POST
    @Path(AGGREGATE_COUNTS_INFIX + INBOX_UNREAD_QUERY_ENDPOINT + "/{partitionId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response filterInboxStreamUnread(@PathParam("partitionId") int id, MiruAggregateCountsQueryAndResultParams params) {
        MiruPartitionId partitionId = MiruPartitionId.of(id);
        try {
            AggregateCountsResult result = miruReader.filterInboxStreamUnread(partitionId, params.getQuery(), params.getLastResult());

            return responseHelper.jsonResponse(result != null ? result : AggregateCountsResult.EMPTY_RESULTS);
        } catch (MiruPartitionUnavailableException e) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Partition unavailable").build();
        } catch (Exception e) {
            log.error("Failed to filter inbox stream unread for partition: " + partitionId.getId(), e);
            return Response.serverError().build();
        }
    }

    @POST
    @Path(DISTINCT_COUNT_INFIX + CUSTOM_QUERY_ENDPOINT)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response countCustomStream(MiruDistinctCountQueryParams params) {
        try {
            DistinctCountResult result = miruReader.countCustomStream(
                params.getTenantId(),
                params.getUserIdentity(),
                params.getAuthzExpression(),
                params.getQueryCriteria());

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
    @Path(DISTINCT_COUNT_INFIX + INBOX_ALL_QUERY_ENDPOINT)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response countInboxStreamAll(MiruDistinctCountQueryParams params) {
        try {
            DistinctCountResult result = miruReader.countInboxStreamAll(
                params.getTenantId(),
                params.getUserIdentity(),
                params.getAuthzExpression(),
                params.getQueryCriteria());

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
    @Path(DISTINCT_COUNT_INFIX + INBOX_UNREAD_QUERY_ENDPOINT)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response countInboxStreamUnread(MiruDistinctCountQueryParams params) {
        try {
            DistinctCountResult result = miruReader.countInboxStreamUnread(
                params.getTenantId(),
                params.getUserIdentity(),
                params.getAuthzExpression(),
                params.getQueryCriteria());

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
    @Path(DISTINCT_COUNT_INFIX + CUSTOM_QUERY_ENDPOINT + "/{partitionId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response countCustomStream(@PathParam("partitionId") int id, MiruDistinctCountQueryAndResultParams params) {
        MiruPartitionId partitionId = MiruPartitionId.of(id);
        try {
            DistinctCountResult result = miruReader.countCustomStream(partitionId, params.getQuery(), params.getLastResult());

            return responseHelper.jsonResponse(result != null ? result : DistinctCountResult.EMPTY_RESULTS);
        } catch (MiruPartitionUnavailableException e) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Partition unavailable").build();
        } catch (Exception e) {
            log.error("Failed to gather counts for custom stream for partition: " + partitionId.getId(), e);
            return Response.serverError().build();
        }
    }

    @POST
    @Path(DISTINCT_COUNT_INFIX + INBOX_ALL_QUERY_ENDPOINT + "/{partitionId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response countInboxStreamAll(@PathParam("partitionId") int id, MiruDistinctCountQueryAndResultParams params) {
        MiruPartitionId partitionId = MiruPartitionId.of(id);
        try {
            DistinctCountResult result = miruReader.countInboxStreamAll(partitionId, params.getQuery(), params.getLastResult());

            return responseHelper.jsonResponse(result != null ? result : DistinctCountResult.EMPTY_RESULTS);
        } catch (MiruPartitionUnavailableException e) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Partition unavailable").build();
        } catch (Exception e) {
            log.error("Failed to gather counts for inbox for partition: " + partitionId.getId(), e);
            return Response.serverError().build();
        }
    }

    @POST
    @Path(DISTINCT_COUNT_INFIX + INBOX_UNREAD_QUERY_ENDPOINT + "/{partitionId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response countInboxStreamUnread(@PathParam("partitionId") int id, MiruDistinctCountQueryAndResultParams params) {
        MiruPartitionId partitionId = MiruPartitionId.of(id);
        try {
            DistinctCountResult result = miruReader.countInboxStreamUnread(partitionId, params.getQuery(), params.getLastResult());

            return responseHelper.jsonResponse(result != null ? result : DistinctCountResult.EMPTY_RESULTS);
        } catch (MiruPartitionUnavailableException e) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Partition unavailable").build();
        } catch (Exception e) {
            log.error("Failed to gather counts for inbox (unread) for partition: " + partitionId.getId(), e);
            return Response.serverError().build();
        }
    }

    @POST
    @Path(TRENDING_INFIX + CUSTOM_QUERY_ENDPOINT)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response scoreTrending(MiruTrendingQueryParams params) {
        try {
            long t = System.currentTimeMillis();
            TrendingResult result = miruReader.scoreTrending(
                params.getTenantId(),
                params.getUserIdentity(),
                params.getAuthzExpression(),
                params.getQueryCriteria());

            log.info("scoreTrending: " + result.results.size() + " / " + result.collectedDistincts + " in " + (System.currentTimeMillis() - t) + " ms");
            return responseHelper.jsonResponse(result != null ? result : TrendingResult.EMPTY_RESULTS);
        } catch (MiruPartitionUnavailableException e) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Partition unavailable").build();
        } catch (Exception e) {
            log.error("Failed to score trending.", e);
            return Response.serverError().build();
        }
    }

    @POST
    @Path(TRENDING_INFIX + CUSTOM_QUERY_ENDPOINT + "/{partitionId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response scoreTrending(@PathParam("partitionId") int id, MiruTrendingQueryAndResultParams params) {
        MiruPartitionId partitionId = MiruPartitionId.of(id);
        try {
            TrendingResult result = miruReader.scoreTrending(partitionId, params.getQuery(), params.getLastResult());

            //log.info("scoreTrending: " + result.collectedDistincts);
            return responseHelper.jsonResponse(result != null ? result : DistinctCountResult.EMPTY_RESULTS);
        } catch (MiruPartitionUnavailableException e) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Partition unavailable").build();
        } catch (Exception e) {
            log.error("Failed to score trending for partition: " + partitionId.getId(), e);
            return Response.serverError().build();
        }
    }

    @POST
    @Path(WARM_ENDPOINT)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response warm(MiruTenantId tenantId) {
        try {
            miruReader.warm(tenantId);
            return responseHelper.jsonResponse("");
        } catch (Exception e) {
            log.error("Failed to warm.", e);
            return Response.serverError().build();
        }
    }

}
