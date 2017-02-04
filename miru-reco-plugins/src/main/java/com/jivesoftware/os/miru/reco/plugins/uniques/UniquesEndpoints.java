package com.jivesoftware.os.miru.reco.plugins.uniques;

import com.jivesoftware.os.miru.plugin.partition.MiruPartitionUnavailableException;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.shared.ResponseHelper;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static com.jivesoftware.os.miru.reco.plugins.uniques.UniquesConstants.CUSTOM_QUERY_ENDPOINT;
import static com.jivesoftware.os.miru.reco.plugins.uniques.UniquesConstants.UNIQUES_PREFIX;

@Singleton
@Path(UNIQUES_PREFIX)
public class UniquesEndpoints {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final UniquesInjectable injectable;
    private final ResponseHelper responseHelper = ResponseHelper.INSTANCE;

    public UniquesEndpoints(@Context UniquesInjectable injectable) {
        this.injectable = injectable;
    }

    @POST
    @Path(CUSTOM_QUERY_ENDPOINT)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response gatherUniques(MiruRequest<UniquesQuery> request) {
        try {
            long t = System.currentTimeMillis();
            MiruResponse<UniquesAnswer> response = injectable.gatherUniques(request);

            if (response.answer != null) {
                log.info("gatherUniques {}:{} in {}ms for tenant {}",
                        request.query.gatherUniquesForField,
                        response.answer.uniques,
                        System.currentTimeMillis() - t,
                        request.tenantId);
            } else {
                log.warn("gatherUniques: no answer for tenant {}", request.tenantId);
            }

            return responseHelper.jsonResponse(response);
        } catch (MiruPartitionUnavailableException | InterruptedException e) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Unavailable " + e.getMessage()).build();
        } catch (Exception e) {
            log.error("Failed to gather uniques.", e);
            return Response.serverError().build();
        }
    }

}
