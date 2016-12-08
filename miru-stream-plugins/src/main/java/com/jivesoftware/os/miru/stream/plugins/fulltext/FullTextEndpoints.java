package com.jivesoftware.os.miru.stream.plugins.fulltext;

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

import static com.jivesoftware.os.miru.stream.plugins.fulltext.FullTextConstants.FULLTEXT_PREFIX;


@Path(FULLTEXT_PREFIX)
@Singleton
public class FullTextEndpoints {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();
    private static final FSTConfiguration conf = FSTConfiguration.createDefaultConfiguration();

    private final FullTextInjectable injectable;
    private final ResponseHelper responseHelper = ResponseHelper.INSTANCE;

    public FullTextEndpoints(@Context FullTextInjectable injectable) {
        this.injectable = injectable;
    }

    @POST
    @Path(FullTextConstants.CUSTOM_QUERY_ENDPOINT)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response filterCustomStream(MiruRequest<FullTextQuery> request) {
        try {
            MiruResponse<FullTextAnswer> result = injectable.filterCustomStream(request);

            //log.info("filterCustomStream: " + answer.collectedDistincts);
            return responseHelper.jsonResponse(result);
        } catch (MiruPartitionUnavailableException | InterruptedException e) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Unavailable " + e.getMessage()).build();
        } catch (Exception e) {
            log.error("Failed to filter custom stream.", e);
            return Response.serverError().build();
        }
    }

    @POST
    @Path(FullTextConstants.CUSTOM_QUERY_ENDPOINT + "/{partitionId}")
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response filterCustomStream(@PathParam("partitionId") int id, byte[] rawBytes) {
        MiruPartitionId partitionId = MiruPartitionId.of(id);
        try {
            MiruRequestAndReport<FullTextQuery, FullTextReport> requestAndReport = (MiruRequestAndReport<FullTextQuery, FullTextReport>) conf.asObject(
                rawBytes);
            MiruPartitionResponse<FullTextAnswer> result = injectable.filterCustomStream(partitionId, requestAndReport);

            return responseHelper.jsonResponse(result != null ? result : new MiruPartitionResponse<>(FullTextAnswer.EMPTY_RESULTS, null));
        } catch (MiruPartitionUnavailableException | InterruptedException e) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Unavailable " + e.getMessage()).build();
        } catch (Exception e) {
            log.error("Failed to filter custom stream for partition: " + partitionId.getId(), e);
            return Response.serverError().build();
        }
    }
}
