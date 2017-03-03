package com.jivesoftware.os.miru.bot.deployable;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.miru.bot.deployable.MiruBotDistinctsInitializer.MiruBotDistinctsConfig;
import com.jivesoftware.os.miru.bot.deployable.MiruBotUniquesInitializer.MiruBotUniquesConfig;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.shared.ResponseHelper;
import io.swagger.annotations.Api;
import java.util.concurrent.Executors;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Api(value = "MiruBotBucket")
@Singleton
@Path("/bot/bucket")
public class MiruBotBucketEndpoints {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruBotDistinctsService miruBotDistinctsService;
    private final MiruBotUniquesService miruBotUniquesService;

    private final ResponseHelper responseHelper = ResponseHelper.INSTANCE;

    public MiruBotBucketEndpoints(
        @Context MiruBotDistinctsService miruBotDistinctsService,
        @Context MiruBotUniquesService miruBotUniquesService) {
        this.miruBotDistinctsService = miruBotDistinctsService;
        this.miruBotUniquesService = miruBotUniquesService;
    }

    private Response safeSnapshot(MiruBotBucketSnapshot miruBotBucketSnapshot) {
        if (miruBotBucketSnapshot == null) {
            return Response.ok("").build();
        }
        return responseHelper.jsonResponse(miruBotBucketSnapshot);
    }

    @GET
    @Path("/distincts")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getDistinctsBuckets() throws Exception {
        try {
            return safeSnapshot(miruBotDistinctsService.genMiruBotBucketSnapshot());
        } catch (Throwable t) {
            LOG.error("Error getting bot buckets", t);
            return Response.serverError().build();
        }
    }

    @GET
    @Path("/uniques")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getUniquesBuckets() throws Exception {
        try {
            return safeSnapshot(miruBotUniquesService.genMiruBotBucketSnapshot());
        } catch (Throwable t) {
            LOG.error("Error getting bot buckets", t);
            return Response.serverError().build();
        }
    }

    @POST
    @Path("/distincts")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response newDistinctsBucket(MiruBotBucketRequest miruBotBucketRequest) throws Exception {
        try {
            MiruBotDistinctsConfig miruBotDistinctsConfig =
                MiruBotBucketRequest.genDistinctsConfig(miruBotBucketRequest);

            Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setNameFormat("mirubot-adhoc-distincts-%d").build()).submit(
                miruBotDistinctsService.createWithConfig(miruBotDistinctsConfig));

            return Response.accepted().build();
        } catch (Throwable t) {
            LOG.error("Error create bot bucket", t);
            return Response.serverError().build();
        }
    }

    @POST
    @Path("/uniques")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response newUniquesBucket(MiruBotBucketRequest miruBotBucketRequest) throws Exception {
        try {
            MiruBotUniquesConfig miruBotUniquesConfig =
                MiruBotBucketRequest.genUniquesConfig(miruBotBucketRequest);

            Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setNameFormat("mirubot-adhoc-uniques-%d").build()).submit(
                miruBotUniquesService.createWithConfig(miruBotUniquesConfig));

            return Response.accepted().build();
        } catch (Throwable t) {
            LOG.error("Error create bot bucket", t);
            return Response.serverError().build();
        }
    }

}
