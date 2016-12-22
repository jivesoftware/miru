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
package com.jivesoftware.os.miru.writer.deployable.endpoints;

import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.api.MiruWriterEndpointConstants;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruReadEvent;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.writer.deployable.base.MiruActivityIngress;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.health.api.HealthFactory;
import com.jivesoftware.os.routing.bird.health.api.HealthTimer;
import com.jivesoftware.os.routing.bird.health.api.TimerHealthCheckConfig;
import com.jivesoftware.os.routing.bird.health.checkers.TimerHealthChecker;
import com.jivesoftware.os.routing.bird.shared.ResponseHelper;
import java.util.List;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.merlin.config.defaults.DoubleDefault;
import org.merlin.config.defaults.IntDefault;
import org.merlin.config.defaults.StringDefault;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author jonathan
 */
@Singleton
@Path(MiruWriterEndpointConstants.INGRESS_PREFIX)
public class MiruIngressEndpoints {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruActivityIngress activityIngress;
    private final ResponseHelper responseHelper = ResponseHelper.INSTANCE;
    private final MiruStats miruStats;

    public MiruIngressEndpoints(@Context MiruActivityIngress activityIngress,
        @Context MiruStats miruStats) {
        this.activityIngress = activityIngress;
        this.miruStats = miruStats;
    }

    static interface IngressHealth extends TimerHealthCheckConfig {

        @StringDefault("http>ingress")
        @Override
        String getName();

        @DoubleDefault(30000d) /// 30sec
        @Override
        Double get95ThPecentileMax();

        @IntDefault(50)
        @Override
        Integer getSampleWindowSize();
    }

    private final HealthTimer ingressHealthTimer = HealthFactory.getHealthTimer(IngressHealth.class, TimerHealthChecker.FACTORY);

    @POST
    @Path(MiruWriterEndpointConstants.ADD)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response ingressActivities(List<MiruActivity> activities) {
        if (activities == null) {
            return Response
                .status(Response.Status.BAD_REQUEST)
                .entity("Activities list must not be null.")
                .build();
        }

        try {
            ingressHealthTimer.startTimer();
            long start = System.currentTimeMillis();
            activityIngress.sendActivity(activities, false);
            long latency = System.currentTimeMillis() - start;
            for (MiruActivity activity : activities) {
                if (activity != null && activity.tenantId != null) {
                    miruStats.ingressed(activity.tenantId.toString(), 1, latency);
                }
            }
            return responseHelper.jsonResponse("Success");
        } catch (Exception e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Failed to add activities.", e);
            } else {
                LOG.error("Failed to add activities.");
            }
            return Response.serverError().build();
        } finally {
            ingressHealthTimer.stopTimer("Activity Ingress Latency", " Add more capacity. Increase batching. Look for down stream issue.");
        }
    }

    static interface RemoveHealth extends TimerHealthCheckConfig {

        @StringDefault("http>remove")
        @Override
        String getName();

        @DoubleDefault(30000d) /// 30sec
        @Override
        Double get95ThPecentileMax();

        @IntDefault(50)
        @Override
        Integer getSampleWindowSize();
    }

    private final HealthTimer removeHealthTimer = HealthFactory.getHealthTimer(RemoveHealth.class, TimerHealthChecker.FACTORY);

    @POST
    @Path(MiruWriterEndpointConstants.REMOVE)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response removeActivities(List<MiruActivity> activities) {
        if (activities == null) {
            return Response
                .status(Response.Status.BAD_REQUEST)
                .entity("Activities list must not be null.")
                .build();
        }

        try {
            removeHealthTimer.startTimer();
            activityIngress.removeActivity(activities);
            return responseHelper.jsonResponse("Success");
        } catch (Exception e) {
            LOG.error("Failed to remove activities.", e);
            return Response.serverError().build();
        } finally {
            removeHealthTimer.stopTimer("Activity Removal Latency", " Add more capacity. Increase batching. Look for down stream issue.");
        }
    }

    static interface IngressReadAllHealth extends TimerHealthCheckConfig {

        @StringDefault("http>ingressReadAll")
        @Override
        String getName();

        @DoubleDefault(30000d) /// 30sec
        @Override
        Double get95ThPecentileMax();

        @IntDefault(50)
        @Override
        Integer getSampleWindowSize();
    }

    private final HealthTimer ingressReadAllTimer = HealthFactory.getHealthTimer(IngressReadAllHealth.class, TimerHealthChecker.FACTORY);

    @POST
    @Path(MiruWriterEndpointConstants.READALL)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
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
                activityIngress.sendAllRead(event);
            }
            return responseHelper.jsonResponse("Success");
        } catch (Exception e) {
            LOG.error("Failed to read activities.", e);
            return Response.serverError().build();
        } finally {
            ingressReadAllTimer.stopTimer("Read All Latency", " Add more capacity. Increase batching. Look for down stream issue.");
        }
    }

    static interface IngressReadHealth extends TimerHealthCheckConfig {

        @StringDefault("http>ingressRead")
        @Override
        String getName();

        @DoubleDefault(30000d) /// 30sec
        @Override
        Double get95ThPecentileMax();

        @IntDefault(50)
        @Override
        Integer getSampleWindowSize();
    }

    private HealthTimer ingressReadTimer = HealthFactory.getHealthTimer(IngressReadHealth.class, TimerHealthChecker.FACTORY);

    @POST
    @Path(MiruWriterEndpointConstants.READ)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
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
                activityIngress.sendRead(event);
            }
            return responseHelper.jsonResponse("Success");
        } catch (Exception e) {
            LOG.error("Failed to read activities.", e);
            return Response.serverError().build();
        } finally {
            ingressReadTimer.stopTimer("Ingress Read Event Latency", " Add more capacity. Increase batching. Look for down stream issue.");
        }
    }

    static interface IngressUnreadHealth extends TimerHealthCheckConfig {

        @StringDefault("http>ingressUnread")
        @Override
        String getName();

        @DoubleDefault(30000d) /// 30sec
        @Override
        Double get95ThPecentileMax();

        @IntDefault(50)
        @Override
        Integer getSampleWindowSize();
    }

    private final HealthTimer ingressUnreadTimer = HealthFactory.getHealthTimer(IngressUnreadHealth.class, TimerHealthChecker.FACTORY);

    @POST
    @Path(MiruWriterEndpointConstants.UNREAD)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
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
                activityIngress.sendUnread(event);
            }
            return responseHelper.jsonResponse("Success");
        } catch (Exception e) {
            LOG.error("Failed to unread activities.", e);
            return Response.serverError().build();
        } finally {
            ingressUnreadTimer.stopTimer("Ingress Unread Event Latency", " Add more capacity. Increase batching. Look for down stream issue.");
        }
    }

    @POST
    @Path("/cursor/{writerId}/{tenantId}/{partitionId}/{index}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response cursor(@PathParam("writerId") int writerId,
        @PathParam("tenantId") String tenantId,
        @PathParam("partitionId") int partitionId,
        @PathParam("index") int index) {

        try {
            activityIngress.updateCursor(writerId, new MiruTenantId(tenantId.getBytes(UTF_8)), MiruPartitionId.of(partitionId), index);
            return responseHelper.jsonResponse("Success");
        } catch (Exception e) {
            LOG.error("Failed to update cursor.", e);
            return Response.serverError().build();
        }
    }
}
