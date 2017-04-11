package com.jivesoftware.os.miru.stream.plugins.strut;

import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.plugin.partition.MiruPartitionUnavailableException;
import com.jivesoftware.os.miru.plugin.solution.MiruPartitionResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruRequestAndReport;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.shared.ResponseHelper;
import java.util.Collections;
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
@Path(StrutConstants.STRUT_PREFIX)
public class StrutEndpoints {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private static final FSTConfiguration conf = FSTConfiguration.createDefaultConfiguration();

    private final StrutInjectable injectable;
    private final ResponseHelper responseHelper = ResponseHelper.INSTANCE;

    public StrutEndpoints(@Context StrutInjectable injectable) {
        this.injectable = injectable;
    }

    @POST
    @Path(StrutConstants.CUSTOM_QUERY_ENDPOINT)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response strutCustom(MiruRequest<StrutQuery> request) {
        try {
            MiruResponse<StrutAnswer> result = injectable.strut(request);

            //log.info("strutCustom: " + answer.collectedDistincts);
            return responseHelper.jsonResponse(result);
        } catch (MiruPartitionUnavailableException | InterruptedException e) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Unavailable " + e.getMessage()).build();
        } catch (Exception e) {
            log.error("Failed to custom strut.", e);
            return Response.serverError().build();
        }
    }

    @POST
    @Path(StrutConstants.CUSTOM_QUERY_ENDPOINT + "/{partitionId}")
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response strutCustom(@PathParam("partitionId") int id, byte[] rawBytes) {
        MiruRequestAndReport<StrutQuery, StrutReport> requestAndReport;
        try {
            requestAndReport = (MiruRequestAndReport<StrutQuery, StrutReport>) conf.asObject(rawBytes);
        } catch (Exception e) {
            log.error("Failed to deserialize request", e);
            return Response.serverError().build();
        }

        try {
            MiruPartitionId partitionId = MiruPartitionId.of(id);
            MiruPartitionResponse<StrutAnswer> result = injectable.strut(partitionId, requestAndReport);
            byte[] responseBytes = result != null ? conf.asByteArray(result) : new byte[0];
            return Response.ok(responseBytes, MediaType.APPLICATION_OCTET_STREAM).build();
        } catch (MiruPartitionUnavailableException | InterruptedException e) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Unavailable " + e.getMessage()).build();
        } catch (Exception e) {
            log.error("Failed to custom strut for partition: {}", new Object[] { id }, e);
            return Response.serverError().build();
        }
    }

    @POST
    @Path(StrutConstants.SHARE_ENDPOINT)
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response strutShare(byte[] rawBytes) {
        StrutShare share;
        try {
            MiruRequestAndReport<StrutShare, Void> requestAndReport = (MiruRequestAndReport<StrutShare, Void>) conf.asObject(rawBytes);
            share = requestAndReport.request.query; // kill me
        } catch (Exception e) {
            log.error("Failed to deserialize request", e);
            return Response.serverError().build();
        }

        try {
            injectable.share(share);
            MiruPartitionResponse<String> result = new MiruPartitionResponse<>("success", Collections.emptyList());
            byte[] responseBytes = conf.asByteArray(result);
            return Response.ok(responseBytes, MediaType.APPLICATION_OCTET_STREAM).build();
        } catch (MiruPartitionUnavailableException e) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Unavailable " + e.getMessage()).build();
        } catch (Exception e) {
            String catwalkId = share.catwalkDefinition != null ? share.catwalkDefinition.catwalkId : null;
            log.error("Failed to share strut for tenantId:{} partitionId:{} catwalkId:{} scorables:{}",
                new Object[] { share.tenantId, share.partitionId, catwalkId, share.scorables.size() }, e);
            return Response.serverError().build();
        }
    }
}
