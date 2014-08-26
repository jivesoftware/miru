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

import com.jivesoftware.os.jive.utils.jaxrs.util.ResponseHelper;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.MiruReadEvent;
import com.jivesoftware.os.miru.client.MiruClient;
import java.util.List;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static com.jivesoftware.os.miru.client.endpoints.MiruClientConstants.CLIENT_SERVICE_ENDPOINT_PREFIX;

/**
 *
 * @author jonathan
 */
@Singleton
@Path(CLIENT_SERVICE_ENDPOINT_PREFIX)
public class MiruClientEndpoints {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final MiruClient client;
    private final ResponseHelper responseHelper = ResponseHelper.INSTANCE;

    public MiruClientEndpoints(@Context MiruClient client) {
        this.client = client;
    }

    @POST
    @Path("ingress")
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
            client.sendActivity(activities, false);
            return responseHelper.jsonResponse("Success");
        } catch (Exception e) {
            log.error("Failed to add activities.", e);
            return Response.serverError().build();
        }
    }

    @POST
    @Path("remove")
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
            client.removeActivity(activities);
            return responseHelper.jsonResponse("Success");
        } catch (Exception e) {
            log.error("Failed to remove activities.", e);
            return Response.serverError().build();
        }
    }

    @POST
    @Path("ingressReadAll")
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
            for (MiruReadEvent event : events) {
                client.sendAllRead(event);
            }
            return responseHelper.jsonResponse("Success");
        } catch (Exception e) {
            log.error("Failed to read activities.", e);
            return Response.serverError().build();
        }
    }

    @POST
    @Path("ingressRead")
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
            for (MiruReadEvent event : events) {
                client.sendRead(event);
            }
            return responseHelper.jsonResponse("Success");
        } catch (Exception e) {
            log.error("Failed to read activities.", e);
            return Response.serverError().build();
        }
    }

    @POST
    @Path("ingressUnread")
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
            for (MiruReadEvent event : events) {
                client.sendRead(event);
            }
            return responseHelper.jsonResponse("Success");
        } catch (Exception e) {
            log.error("Failed to unread activities.", e);
            return Response.serverError().build();
        }
    }

}
