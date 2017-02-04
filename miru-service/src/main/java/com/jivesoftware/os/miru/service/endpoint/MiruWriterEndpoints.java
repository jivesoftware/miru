package com.jivesoftware.os.miru.service.endpoint;

import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.service.MiruService;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.shared.ResponseHelper;
import java.util.List;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static com.jivesoftware.os.miru.api.MiruReaderEndpointConstants.ADD_ACTIVITIES;
import static com.jivesoftware.os.miru.api.MiruReaderEndpointConstants.WRITER_SERVICE_ENDPOINT_PREFIX;

@Singleton
@Path(WRITER_SERVICE_ENDPOINT_PREFIX)
public class MiruWriterEndpoints {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final MiruService service;
    private final MiruStats stats;
    private final ResponseHelper responseHelper = ResponseHelper.INSTANCE;

    public MiruWriterEndpoints(@Context MiruService service, @Context MiruStats stats) {
        this.service = service;
        this.stats = stats;
    }

    @POST
    @Path(ADD_ACTIVITIES)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response addActivities(List<MiruPartitionedActivity> activities) {
        if (activities == null) {
            return Response
                .status(Response.Status.BAD_REQUEST)
                .entity("Activities list must not be null.")
                .build();
        }

        try {
            long start = System.currentTimeMillis();
            service.writeToIndex(activities);
            stats.ingressed(ADD_ACTIVITIES, activities.size(), System.currentTimeMillis() - start);
            return responseHelper.jsonResponse("Success");
        } catch (Exception e) {
            log.error("Failed to add activities.", e);
            return Response.serverError().build();
        }
    }

}
