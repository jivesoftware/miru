package com.jivesoftware.os.miru.bot.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.shared.ResponseHelper;
import io.swagger.annotations.Api;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Api(value="MiruBotBucket")
@Singleton
@Path("/bot")
public class MiruBotBucketEndpoints {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruBotDistinctsService miruBotDistinctsService;
    private final MiruBotUniquesService miruBotUniquesService;
    private final ObjectMapper objectMapper;

    private final ResponseHelper responseHelper = ResponseHelper.INSTANCE;

    public MiruBotBucketEndpoints(
            @Context MiruBotDistinctsService miruBotDistinctsService,
            @Context MiruBotUniquesService miruBotUniquesService,
            @Context ObjectMapper objectMapper) {
        this.miruBotDistinctsService = miruBotDistinctsService;
        this.miruBotUniquesService = miruBotUniquesService;
        this.objectMapper = objectMapper;
    }

    @GET
    @Path("/bucket")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getBuckets() throws Exception {
        try {
            // return known bot buckets
            return Response.ok(Lists.newArrayList()).build();
        } catch (Throwable t) {
            LOG.error("Error getting bot buckets", t);
            return Response.serverError().build();
        }
    }

    @POST
    @Path("/bucket")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response newBucket(MiruBotBucketRequest request) throws Exception {
        try {
            // create new bot bucket using request
            return Response.accepted().build();
        } catch (Throwable t) {
            LOG.error("Error create bot bucket", t);
            return Response.serverError().build();
        }
    }

}
