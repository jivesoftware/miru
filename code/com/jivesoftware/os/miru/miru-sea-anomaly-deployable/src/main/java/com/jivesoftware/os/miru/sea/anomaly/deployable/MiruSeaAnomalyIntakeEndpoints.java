package com.jivesoftware.os.miru.sea.anomaly.deployable;

import com.jivesoftware.os.miru.metric.sampler.MiruMetricSampleEvent;
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
@Path("/miru/seaAnomaly")
public class MiruSeaAnomalyIntakeEndpoints {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruSeaAnomalyIntakeService intakeService;

    public MiruSeaAnomalyIntakeEndpoints(@Context MiruSeaAnomalyIntakeService intakeService) {
        this.intakeService = intakeService;
    }

    @POST
    @Path("/intake")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_HTML)
    public Response intake(List<MiruMetricSampleEvent> events) throws Exception {
        try {
            intakeService.ingressEvents(events);
            return Response.ok().build();
        } catch (Throwable t) {
            LOG.error("Error on intake for {} events", new Object[] { events.size() }, t);
            return Response.serverError().build();
        }
    }

}
