package com.jivesoftware.os.miru.stream.plugins.catwalk;

import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.catwalk.shared.CatwalkQuery;
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

@Singleton
@Path(CatwalkConstants.CATWALK_PREFIX)
public class CatwalkEndpoints {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private static final FSTConfiguration conf = FSTConfiguration.createDefaultConfiguration();

    private final CatwalkInjectable injectable;
    private final ResponseHelper responseHelper = ResponseHelper.INSTANCE;

    public CatwalkEndpoints(@Context CatwalkInjectable injectable) {
        this.injectable = injectable;
    }

    @POST
    @Path(CatwalkConstants.CUSTOM_QUERY_ENDPOINT)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response catwalkCustom(MiruRequest<CatwalkQuery> request) {
        try {
            MiruResponse<CatwalkAnswer> result = injectable.strut(request);

            //log.info("catwalkCustom: " + answer.collectedDistincts);
            return responseHelper.jsonResponse(result);
        } catch (MiruPartitionUnavailableException | InterruptedException e) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Unavailable " + e.getMessage()).build();
        } catch (Exception e) {
            log.error("Failed to custom catwalk.", e);
            return Response.serverError().build();
        }
    }

    @POST
    @Path(CatwalkConstants.CUSTOM_QUERY_ENDPOINT + "/{partitionId}")
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response catwalkCustom(@PathParam("partitionId") int id, byte[] rawBytes) {
        MiruRequestAndReport<CatwalkQuery, CatwalkReport> requestAndReport;
        try {
            requestAndReport = (MiruRequestAndReport<CatwalkQuery, CatwalkReport>) conf.asObject(rawBytes);
        } catch (Exception e) {
            log.error("Failed to deserialize request", e);
            return Response.serverError().build();
        }

        try {
            MiruPartitionId partitionId = MiruPartitionId.of(id);
            MiruPartitionResponse<CatwalkAnswer> result = injectable.strut(partitionId, requestAndReport);
            byte[] responseBytes = result != null ? conf.asByteArray(result) : new byte[0];
            return Response.ok(responseBytes, MediaType.APPLICATION_OCTET_STREAM).build();
        } catch (MiruPartitionUnavailableException | InterruptedException e) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Unavailable " + e.getMessage()).build();
        } catch (Exception e) {
            log.error("Failed to custom catwalk for partition: {}", new Object[] { id }, e);
            return Response.serverError().build();
        }
    }

    @POST
    @Path(CatwalkConstants.PARTITION_QUERY_ENDPOINT + "/{partitionId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response catwalkCustomPartition(@PathParam("partitionId") int id,
        MiruRequest<CatwalkQuery> request) {
        try {
            MiruPartitionId partitionId = MiruPartitionId.of(id);
            MiruResponse<CatwalkAnswer> result = injectable.strut(partitionId, request);

            //log.info("catwalkPartition: " + answer.collectedDistincts);
            return responseHelper.jsonResponse(result);
        } catch (MiruPartitionUnavailableException | InterruptedException e) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Unavailable " + e.getMessage()).build();
        } catch (Exception e) {
            log.error("Failed to partition catwalk for partition: {}", new Object[] { id }, e);
            return Response.serverError().build();
        }
    }
}
