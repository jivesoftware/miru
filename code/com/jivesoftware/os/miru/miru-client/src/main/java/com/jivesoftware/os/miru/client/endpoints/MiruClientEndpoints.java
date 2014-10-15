/*
 * Copyright 2014 Jive Software Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jivesoftware.os.miru.client.endpoints;

import com.jivesoftware.os.jive.utils.health.api.HealthFactory;
import com.jivesoftware.os.jive.utils.health.api.HealthTimer;
import com.jivesoftware.os.jive.utils.health.api.TimerHealthCheckConfig;
import com.jivesoftware.os.jive.utils.health.checkers.TimerHealthChecker;
import com.jivesoftware.os.jive.utils.jaxrs.util.ResponseHelper;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.MiruReadEvent;
import com.jivesoftware.os.miru.client.MiruClient;
import java.util.List;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.merlin.config.defaults.StringDefault;

import static com.jivesoftware.os.miru.client.endpoints.MiruClientConstants.CLIENT_SERVICE_ENDPOINT_PREFIX;

/**
 *
 * @author jonathan
 */
@Path (CLIENT_SERVICE_ENDPOINT_PREFIX)
public class MiruClientEndpoints {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final MiruClient client;
    private final ResponseHelper responseHelper = ResponseHelper.INSTANCE;

    public MiruClientEndpoints(@Context MiruClient client) {
        this.client = client;
    }

    static interface IngressHealth extends TimerHealthCheckConfig {

        @StringDefault ("http>ingress")
        @Override
        String getName();
    }

    private final HealthTimer ingressHealthTimer = HealthFactory.getHealthTimer(IngressHealth.class, TimerHealthChecker.FACTORY);

    @POST
    @Path ("ingress")
    @Consumes (MediaType.APPLICATION_JSON)
    @Produces (MediaType.APPLICATION_JSON)
    public Response ingressActivities(List<MiruActivity> activities) {
        if (activities == null) {
            return Response
                .status(Response.Status.BAD_REQUEST)
                .entity("Activities list must not be null.")
                .build();
        }

        try {
            ingressHealthTimer.startTimer();
            client.sendActivity(activities, false);
            return responseHelper.jsonResponse("Success");
        } catch (Exception e) {
            log.error("Failed to add activities.", e);
            return Response.serverError().build();
        } finally {
            ingressHealthTimer.stopTimer("Its take to long to ingress " + activities.size() + "activities. ");
        }
    }

    static interface RemoveHealth extends TimerHealthCheckConfig {

        @StringDefault ("http>remove")
        @Override
        String getName();
    }
    private final HealthTimer removeHealthTimer = HealthFactory.getHealthTimer(RemoveHealth.class, TimerHealthChecker.FACTORY);

    @POST
    @Path ("remove")
    @Consumes (MediaType.APPLICATION_JSON)
    @Produces (MediaType.APPLICATION_JSON)
    public Response removeActivities(List<MiruActivity> activities) {
        if (activities == null) {
            return Response
                .status(Response.Status.BAD_REQUEST)
                .entity("Activities list must not be null.")
                .build();
        }

        try {
            removeHealthTimer.startTimer();
            client.removeActivity(activities);
            return responseHelper.jsonResponse("Success");
        } catch (Exception e) {
            log.error("Failed to remove activities.", e);
            return Response.serverError().build();
        } finally {
            removeHealthTimer.stopTimer("Its take to long to remove " + activities.size() + "activities. ");
        }
    }

    static interface IngressReadAllHealth extends TimerHealthCheckConfig {

        @StringDefault ("http>ingressReadAll")
        @Override
        String getName();
    }
    private final HealthTimer ingressReadAllTimer = HealthFactory.getHealthTimer(IngressReadAllHealth.class, TimerHealthChecker.FACTORY);

    @POST
    @Path ("ingressReadAll")
    @Consumes (MediaType.APPLICATION_JSON)
    @Produces (MediaType.APPLICATION_JSON)
    public Response ingressReadAll(List<MiruReadEvent> events) {
        if (events == null) {
            return Response
                .status(Response.Status.BAD_REQUEST)
                .entity("Read all events list must not be null.")
                .build();
        }

        try {
            ingressReadAllTimer.startTimer();
            for (MiruReadEvent event : events) {
                client.sendAllRead(event);
            }
            return responseHelper.jsonResponse("Success");
        } catch (Exception e) {
            log.error("Failed to read activities.", e);
            return Response.serverError().build();
        } finally {
            ingressReadAllTimer.stopTimer("Its take to long to ingressReadAll " + events.size() + "events. ");
        }
    }

    static interface IngressReadHealth extends TimerHealthCheckConfig {

        @StringDefault ("http>ingressRead")
        @Override
        String getName();
    }
    private HealthTimer ingressReadTimer = HealthFactory.getHealthTimer(IngressReadHealth.class, TimerHealthChecker.FACTORY);

    @POST
    @Path ("ingressRead")
    @Consumes (MediaType.APPLICATION_JSON)
    @Produces (MediaType.APPLICATION_JSON)
    public Response ingressRead(List<MiruReadEvent> events) {
        if (events == null) {
            return Response
                .status(Response.Status.BAD_REQUEST)
                .entity("Read events list must not be null.")
                .build();
        }

        try {
            ingressReadTimer.startTimer();
            for (MiruReadEvent event : events) {
                client.sendRead(event);
            }
            return responseHelper.jsonResponse("Success");
        } catch (Exception e) {
            log.error("Failed to read activities.", e);
            return Response.serverError().build();
        } finally {
            ingressReadTimer.stopTimer("Its take to long to ingressRead " + events.size() + "events. ");
        }
    }

    static interface IngressUnreadHealth extends TimerHealthCheckConfig {

        @StringDefault ("http>ingressUnread")
        @Override
        String getName();
    }
    private final HealthTimer ingressUnreadTimer = HealthFactory.getHealthTimer(IngressUnreadHealth.class, TimerHealthChecker.FACTORY);

    @POST
    @Path ("ingressUnread")
    @Consumes (MediaType.APPLICATION_JSON)
    @Produces (MediaType.APPLICATION_JSON)
    public Response ingressUnread(List<MiruReadEvent> events) {
        if (events == null) {
            return Response
                .status(Response.Status.BAD_REQUEST)
                .entity("Unread events list must not be null.")
                .build();
        }

        try {
            ingressUnreadTimer.startTimer();
            for (MiruReadEvent event : events) {
                client.sendRead(event);
            }
            return responseHelper.jsonResponse("Success");
        } catch (Exception e) {
            log.error("Failed to unread activities.", e);
            return Response.serverError().build();
        } finally {
            ingressUnreadTimer.stopTimer("Its take to long to ingressUnread " + events.size() + "events. ");
        }
    }

}
