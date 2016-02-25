package com.jivesoftware.os.miru.stream.plugins.catwalk;

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


@Path(CatwalkConstants.CATWALK_PREFIX)
@Singleton
public class CatwalkEndpoints {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

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
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response catwalkCustom(@PathParam("partitionId") int id, MiruRequestAndReport<CatwalkQuery, CatwalkReport> requestAndReport) {
        MiruPartitionId partitionId = MiruPartitionId.of(id);
        try {
            MiruPartitionResponse<CatwalkAnswer> result = injectable.strut(partitionId, requestAndReport);

            return responseHelper.jsonResponse(result != null ? result : new MiruPartitionResponse<>(CatwalkAnswer.EMPTY_RESULTS, null));
        } catch (MiruPartitionUnavailableException | InterruptedException e) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Unavailable " + e.getMessage()).build();
        } catch (Exception e) {
            log.error("Failed to custom catwalk for partition: " + partitionId.getId(), e);
            return Response.serverError().build();
        }
    }
}
