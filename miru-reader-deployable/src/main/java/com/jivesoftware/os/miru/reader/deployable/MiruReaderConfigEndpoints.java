package com.jivesoftware.os.miru.reader.deployable;

import com.google.common.base.Charsets;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.service.MiruService;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.shared.ResponseHelper;
import java.util.concurrent.TimeUnit;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.FormParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Singleton
@Path("/miru/config")
public class MiruReaderConfigEndpoints {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final MiruService miruService;
    private final MiruStats stats;
    private final TimestampedOrderIdProvider timestampedOrderIdProvider;
    private final ResponseHelper responseHelper = ResponseHelper.INSTANCE;

    public MiruReaderConfigEndpoints(@Context MiruService miruService,
        @Context MiruStats stats,
        @Context TimestampedOrderIdProvider timestampedOrderIdProvider) {
        this.miruService = miruService;
        this.stats = stats;
        this.timestampedOrderIdProvider = timestampedOrderIdProvider;
    }

    @POST
    @Path("/rebuild/prioritize/{tenantId}/{partitionId}")
    public Response check(
        @PathParam("tenantId") String tenantId,
        @PathParam("partitionId") Integer partitionId) {
        try {
            long start = System.currentTimeMillis();
            MiruTenantId tenant = new MiruTenantId(tenantId.getBytes(Charsets.UTF_8));
            MiruPartitionId partition = MiruPartitionId.of(partitionId);
            Response response;
            if (miruService.prioritizeRebuild(tenant, partition)) {
                response = responseHelper.jsonResponse("Success");
            } else {
                response = Response.status(Response.Status.NOT_FOUND).build();
            }
            stats.ingressed("POST:/rebuild/prioritize/" + tenantId + "/" + partitionId, 1, System.currentTimeMillis() - start);
            return response;
        } catch (Throwable t) {
            log.error("Failed to prioritize rebuild for tenant {} partition {}", new Object[] { tenantId, partitionId }, t);
            return Response.serverError().build();
        }
    }

    @POST
    @Path("/rebuild")
    @Produces(MediaType.TEXT_HTML)
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public Response rebuild(@FormParam("days") int days,
        @FormParam("hotDeploy") boolean hotDeploy,
        @FormParam("chunkStores") boolean chunkStores,
        @FormParam("labIndexes") boolean labIndexes) {
        try {
            long smallestTimestamp = timestampedOrderIdProvider.getApproximateId(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(days));
            MiruTimeRange miruTimeRange = new MiruTimeRange(smallestTimestamp, Long.MAX_VALUE);
            boolean result = miruService.rebuildTimeRange(miruTimeRange, hotDeploy, chunkStores, labIndexes);
            return Response.ok(result ? "success" : "failure").build();
        } catch (Throwable t) {
            log.error("Failed to rebuild({}, {}, {}, {})", new Object[] { days, hotDeploy, chunkStores, labIndexes }, t);
            return Response.serverError().build();
        }
    }
}
