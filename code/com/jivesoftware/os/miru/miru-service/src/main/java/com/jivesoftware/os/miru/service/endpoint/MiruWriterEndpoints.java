package com.jivesoftware.os.miru.service.endpoint;

import com.jivesoftware.os.jive.utils.jaxrs.util.ResponseHelper;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.MiruWriter;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import java.util.List;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static com.jivesoftware.os.miru.api.MiruWriter.ADD_ACTIVITIES;
import static com.jivesoftware.os.miru.api.MiruWriter.WRITER_SERVICE_ENDPOINT_PREFIX;

@Path(WRITER_SERVICE_ENDPOINT_PREFIX)
public class MiruWriterEndpoints {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final MiruWriter writer;
    private final ResponseHelper responseHelper = ResponseHelper.INSTANCE;

    public MiruWriterEndpoints(@Context MiruWriter writer) {
        this.writer = writer;
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
            writer.writeToIndex(activities);
            return responseHelper.jsonResponse("Success");
        } catch (Exception e) {
            log.error("Failed to add activities.", e);
            return Response.serverError().build();
        }
    }

    @POST
    @Path("/wal/activities")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response writeWAL(List<MiruPartitionedActivity> activities) {
        if (activities == null) {
            return Response
                .status(Response.Status.BAD_REQUEST)
                .entity("Activities list must not be null.")
                .build();
        }

        try {
            writer.writeToWAL(activities);
            return responseHelper.jsonResponse("Success");
        } catch (Exception e) {
            log.error("Failed to write activities.", e);
            return Response.serverError().build();
        }
    }

}
