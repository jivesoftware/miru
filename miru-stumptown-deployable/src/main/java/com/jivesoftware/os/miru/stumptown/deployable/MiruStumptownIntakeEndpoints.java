package com.jivesoftware.os.miru.stumptown.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Objects;
import com.jivesoftware.os.filer.queue.guaranteed.delivery.GuaranteedDeliveryService;
import com.jivesoftware.os.miru.logappender.MiruLogEvent;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;

import java.util.Collections;
import java.util.List;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Singleton
@Path("/miru/stumptown")
public class MiruStumptownIntakeEndpoints {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final IngressGuaranteedDeliveryQueueProvider deliveryQueueProvider;
    private final ObjectMapper mapper;

    public MiruStumptownIntakeEndpoints(@Context IngressGuaranteedDeliveryQueueProvider deliveryQueueProvider, @Context ObjectMapper mapper) {
        this.deliveryQueueProvider = deliveryQueueProvider;
        this.mapper = mapper;
    }

    @POST
    @Path("/intake")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response intake(List<MiruLogEvent> logEvents) throws Exception {
        try {
            for (MiruLogEvent logEvent : logEvents) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("host:{} service:{} instance:{} message:{}",
                            logEvent.host, logEvent.service, logEvent.instance, logEvent.message);
                }

                int hash = Objects.hashCode(logEvent.host, logEvent.service, logEvent.instance, logEvent.threadName);
                GuaranteedDeliveryService guaranteedDeliveryService = deliveryQueueProvider.getGuaranteedDeliveryServices(hash);
                guaranteedDeliveryService.add(Collections.singletonList(mapper.writeValueAsBytes(logEvent)));
            }

            return Response.accepted().build();
        } catch (Throwable t) {
            LOG.error("Error on intake for {} events", new Object[]{logEvents.size()}, t);
            return Response.serverError().build();
        }
    }

}
