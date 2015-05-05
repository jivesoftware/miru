package com.jivesoftware.os.miru.sea.anomaly.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Objects;
import com.jivesoftware.os.filer.queue.guaranteed.delivery.GuaranteedDeliveryService;
import com.jivesoftware.os.miru.metric.sampler.AnomalyMetric;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Collections;
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
@Path("/miru/sea/anomaly")
public class MiruSeaAnomalyIntakeEndpoints {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final IngressGuaranteedDeliveryQueueProvider deliveryQueueProvider;
    private final ObjectMapper mapper;

    public MiruSeaAnomalyIntakeEndpoints(@Context IngressGuaranteedDeliveryQueueProvider deliveryQueueProvider, @Context ObjectMapper mapper) {
        this.deliveryQueueProvider = deliveryQueueProvider;
        this.mapper = mapper;
    }

    @POST
    @Path("/intake")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_HTML)
    public Response intake(List<AnomalyMetric> events) throws Exception {
        try {
            for (AnomalyMetric event : events) {
                int hash = Objects.hashCode(event.host, event.service, event.instance, event.sampler);
                GuaranteedDeliveryService guaranteedDeliveryService = deliveryQueueProvider.getGuaranteedDeliveryServices(hash);
                guaranteedDeliveryService.add(Collections.singletonList(mapper.writeValueAsBytes(event)));
            }
            return Response.ok().build();
        } catch (Throwable t) {
            LOG.error("Error on intake for {} events", new Object[] { events.size() }, t);
            return Response.serverError().build();
        }
    }

}
