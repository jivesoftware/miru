package com.jivesoftware.os.miru.lumberyard.deployable;

import com.jivesoftware.os.miru.logappender.MiruLogEvent;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.List;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 *
 */
@Singleton
@Path("/miru/lumberyard")
public class MiruLumberyardIntakeEndpoints {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruLumberyardIntakeService intakeService;

    public MiruLumberyardIntakeEndpoints(@Context MiruLumberyardIntakeService intakeService) {
        this.intakeService = intakeService;
    }

    @POST
    @Path("/intake")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_HTML)
    public Response intake(List<MiruLogEvent> logEvents) throws Exception {
        try {
            intakeService.ingressLogEvents(logEvents);
            return Response.ok().build();
        } catch (Throwable t) {
            LOG.error("Error on intake for {} events", new Object[] { logEvents.size() }, t);
            return Response.serverError().build();
        }
    }

}
