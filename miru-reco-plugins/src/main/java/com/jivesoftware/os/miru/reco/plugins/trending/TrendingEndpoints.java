package com.jivesoftware.os.miru.reco.plugins.trending;

import com.jivesoftware.os.jive.utils.jaxrs.util.ResponseHelper;
import com.jivesoftware.os.miru.plugin.partition.MiruPartitionUnavailableException;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static com.jivesoftware.os.miru.reco.plugins.trending.TrendingConstants.CUSTOM_QUERY_ENDPOINT;
import static com.jivesoftware.os.miru.reco.plugins.trending.TrendingConstants.TRENDING_PREFIX;

@Path(TRENDING_PREFIX)
public class TrendingEndpoints {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final TrendingInjectable injectable;
    //private final FstMarshaller fstMarshaller;
    private final ResponseHelper responseHelper = ResponseHelper.INSTANCE;

    public TrendingEndpoints(
        @Context TrendingInjectable injectable) {
        //@Context FstMarshaller fstMarshaller) {
        this.injectable = injectable;
        //this.fstMarshaller = fstMarshaller;
    }

    @POST
    @Path(CUSTOM_QUERY_ENDPOINT)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response scoreTrending(MiruRequest<TrendingQuery> request) {
        try {
            long t = System.currentTimeMillis();
            MiruResponse<TrendingAnswer> response = injectable.scoreTrending(request);

            log.info("scoreTrending: " + response.answer.results.size() + " / " + request.query.desiredNumberOfDistincts +
                    " in " + (System.currentTimeMillis() - t) + " ms");
            return responseHelper.jsonResponse(response);
        } catch (MiruPartitionUnavailableException e) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Partition unavailable").build();
        } catch (Exception e) {
            log.error("Failed to score trending.", e);
            return Response.serverError().build();
        }
    }
}
