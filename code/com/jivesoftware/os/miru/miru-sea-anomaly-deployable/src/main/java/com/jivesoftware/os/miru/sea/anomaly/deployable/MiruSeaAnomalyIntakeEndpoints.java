package com.jivesoftware.os.miru.sea.anomaly.deployable;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.jivesoftware.os.filer.queue.guaranteed.delivery.GuaranteedDeliveryService;
import com.jivesoftware.os.miru.metric.sampler.AnomalyMetric;
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
            if (!events.isEmpty()) {
                // as an optimization, key off the first service in the events, since the entire batch likely originates from this service
                String key = events.get(0).service;
                GuaranteedDeliveryService guaranteedDeliveryService = deliveryQueueProvider.getGuaranteedDeliveryServices(key);
                guaranteedDeliveryService.add(Lists.transform(events, new Function<AnomalyMetric, byte[]>() {
                    @Override
                    public byte[] apply(AnomalyMetric input) {
                        try {
                            return mapper.writeValueAsBytes(input);
                        } catch (JsonProcessingException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }));
            }
            return Response.ok().build();
        } catch (Throwable t) {
            LOG.error("Error on intake for {} events", new Object[] { events.size() }, t);
            return Response.serverError().build();
        }
    }

}
