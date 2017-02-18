package com.jivesoftware.os.miru.reco.plugins.distincts;

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

import static com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsConstants.CUSTOM_QUERY_ENDPOINT;
import static com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsConstants.DISTINCTS_PREFIX;

@Singleton
@Path(DISTINCTS_PREFIX)
public class DistinctsEndpoints {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();
    private static final FSTConfiguration conf = FSTConfiguration.createDefaultConfiguration();

    private final DistinctsInjectable injectable;
    private final ResponseHelper responseHelper = ResponseHelper.INSTANCE;

    public DistinctsEndpoints(
        @Context DistinctsInjectable injectable) {
        this.injectable = injectable;
    }

    @POST
    @Path(CUSTOM_QUERY_ENDPOINT)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response gatherDistincts(MiruRequest<DistinctsQuery> request) {
        try {
            long t = System.currentTimeMillis();
            MiruResponse<DistinctsAnswer> response = injectable.gatherDistincts(request);

            if (response.answer == null) {
                log.warn("gatherDistincts: no answer for tenant {}", request.tenantId);
            } else {
                log.info("gatherDistincts {}:{} / {} in {}ms for tenant {}",
                    request.query.gatherDistinctsForField,
                    response.answer.results != null ? response.answer.results.size() : -1,
                    response.answer.collectedDistincts,
                    System.currentTimeMillis() - t,
                    request.tenantId);
            }
            return responseHelper.jsonResponse(response);
        } catch (MiruPartitionUnavailableException | InterruptedException e) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Unavailable " + e.getMessage()).build();
        } catch (Exception e) {
            log.error("Failed to gather distincts for tenant: {}", new Object[] { request.tenantId }, e);
            return Response.serverError().build();
        }
    }

    @POST
    @Path(CUSTOM_QUERY_ENDPOINT + "/{partitionId}")
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response gatherDistincts(@PathParam("partitionId") int id, byte[] rawBytes) {
        MiruPartitionId partitionId = MiruPartitionId.of(id);

        MiruRequestAndReport<DistinctsQuery, DistinctsReport> requestAndReport;
        try {
            requestAndReport = (MiruRequestAndReport<DistinctsQuery, DistinctsReport>) conf.asObject(rawBytes);
        } catch (Exception e) {
            log.error("Failed to deserialize request", e);
            return Response.serverError().build();
        }

        try {
            MiruPartitionResponse<DistinctsAnswer> result = injectable.gatherDistincts(partitionId, requestAndReport);
            byte[] responseBytes = result != null ? conf.asByteArray(result) : new byte[0];
            return Response.ok(responseBytes, MediaType.APPLICATION_OCTET_STREAM).build();
        } catch (MiruPartitionUnavailableException | InterruptedException e) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Unavailable " + e.getMessage()).build();
        } catch (Exception e) {
            log.error("Failed to gather distincts for tenant: {} partition: {}", new Object[] { requestAndReport.request.tenantId, partitionId.getId() }, e);
            return Response.serverError().build();
        }
    }
}
