package com.jivesoftware.os.miru.reco.plugins.reco;

import com.jivesoftware.os.jive.utils.jaxrs.util.ResponseHelper;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.query.MiruPartitionUnavailableException;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static com.jivesoftware.os.miru.reco.plugins.reco.RecoConstants.CUSTOM_QUERY_ENDPOINT;
import static com.jivesoftware.os.miru.reco.plugins.reco.RecoConstants.RECO_PREFIX;

@Path(RECO_PREFIX)
public class RecoEndpoints {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final RecoInjectable injectable;
    private final ResponseHelper responseHelper = ResponseHelper.INSTANCE;

    public RecoEndpoints(@Context RecoInjectable injectable) {
        this.injectable = injectable;
    }

    @POST
    @Path(CUSTOM_QUERY_ENDPOINT)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response collaborativeFiltering(RecoQuery query) {
        try {
            long t = System.currentTimeMillis();
            RecoResult result = injectable.collaborativeFilteringRecommendations(query);

            log.info("collaborativeFiltering: " + result.results.size() + " in " + (System.currentTimeMillis() - t) + " ms");
            return responseHelper.jsonResponse(result != null ? result : RecoResult.EMPTY_RESULTS);
        } catch (MiruPartitionUnavailableException e) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Partition unavailable").build();
        } catch (Exception e) {
            log.error("Failed to score reco.", e);
            return Response.serverError().build();
        }
    }

    @POST
    @Path(CUSTOM_QUERY_ENDPOINT + "/{partitionId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response collaborativeFiltering(@PathParam("partitionId") int id, RecoQueryAndResult queryAndResult) {
        MiruPartitionId partitionId = MiruPartitionId.of(id);
        try {
            RecoResult result = injectable.collaborativeFilteringRecommendations(partitionId, queryAndResult);

            //log.info("collaborativeFiltering: " + result.results.size());
            return responseHelper.jsonResponse(result != null ? result : RecoResult.EMPTY_RESULTS);
        } catch (MiruPartitionUnavailableException e) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Partition unavailable").build();
        } catch (Exception e) {
            log.error("Failed to score reco for partition: " + partitionId.getId(), e);
            return Response.serverError().build();
        }
    }
}
