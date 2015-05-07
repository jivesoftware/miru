package com.jivesoftware.os.miru.service.endpoint;

import com.google.common.base.Charsets;
import com.jivesoftware.os.jive.utils.jaxrs.util.ResponseHelper;
import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.service.MiruService;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.List;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static com.jivesoftware.os.miru.api.MiruReader.INSPECT_ENDPOINT;
import static com.jivesoftware.os.miru.api.MiruReader.QUERY_SERVICE_ENDPOINT_PREFIX;
import static com.jivesoftware.os.miru.api.MiruReader.WARM_ALL_ENDPOINT;
import static com.jivesoftware.os.miru.api.MiruReader.WARM_ENDPOINT;

@Path(QUERY_SERVICE_ENDPOINT_PREFIX)
public class MiruReaderEndpoints {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final MiruService miruService;
    private final MiruStats stats;
    private final ResponseHelper responseHelper = ResponseHelper.INSTANCE;

    public MiruReaderEndpoints(@Context MiruService miruService, @Context MiruStats stats) {
        this.miruService = miruService;
        this.stats = stats;
    }

    @POST
    @Path(WARM_ENDPOINT)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response warm(MiruTenantId tenantId) {
        try {
            long start = System.currentTimeMillis();
            miruService.warm(tenantId);
            stats.ingressed(WARM_ENDPOINT + "/" + tenantId.toString(), 1, System.currentTimeMillis() - start);
            return responseHelper.jsonResponse("");
        } catch (Exception e) {
            log.error("Failed to warm.", e);
            return Response.serverError().build();
        }
    }

    @POST
    @Path(WARM_ALL_ENDPOINT)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response warm(List<MiruTenantId> tenantIds) {
        try {
            long start = System.currentTimeMillis();
            for (MiruTenantId tenantId : tenantIds) {
                try {
                    miruService.warm(tenantId);
                } catch (Exception e) {
                    log.error("Failed to warm tenant {}", new Object[]{tenantId}, e);
                }
            }
            stats.ingressed(WARM_ALL_ENDPOINT, tenantIds.size(), System.currentTimeMillis() - start);
            return responseHelper.jsonResponse("");
        } catch (Exception e) {
            log.error("Failed to warm multiple tenants.", e);
            return Response.serverError().build();
        }
    }

    @GET
    @Path(INSPECT_ENDPOINT + "/{tenantId}/{partitionId}/{field}/{term}")
    @Produces(MediaType.TEXT_PLAIN)
    public Response inspect(@PathParam("tenantId") String tenantIdString,
        @PathParam("partitionId") int partitionId,
        @PathParam("field") String field,
        @PathParam("term") String term) {
        try {
            long start = System.currentTimeMillis();
            MiruTenantId tenantId = new MiruTenantId(tenantIdString.getBytes(Charsets.UTF_8));
            String value = miruService.inspect(tenantId, MiruPartitionId.of(partitionId), field, term);
            stats.ingressed(INSPECT_ENDPOINT + "/" + tenantIdString + "/" + partitionId + "/" + field + "/" + term, 1, System.currentTimeMillis() - start);
            return Response.ok(value).build();
        } catch (Exception e) {
            log.error("Failed to inspect.", e);
            return Response.serverError().build();
        }
    }
}
