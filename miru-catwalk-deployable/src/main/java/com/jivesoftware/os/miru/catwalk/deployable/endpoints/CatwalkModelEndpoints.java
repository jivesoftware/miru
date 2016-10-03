package com.jivesoftware.os.miru.catwalk.deployable.endpoints;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.catwalk.deployable.CatwalkModelService;
import com.jivesoftware.os.miru.catwalk.deployable.CatwalkModelUpdater;
import com.jivesoftware.os.miru.stream.plugins.catwalk.CatwalkModel;
import com.jivesoftware.os.miru.stream.plugins.catwalk.CatwalkQuery;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.BufferedOutputStream;
import java.nio.charset.StandardCharsets;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import org.xerial.snappy.Snappy;
import org.xerial.snappy.SnappyOutputStream;

/**
 *
 */
@Singleton
@Path("/miru/catwalk/model")
public class CatwalkModelEndpoints {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final CatwalkModelService catwalkModelService;
    private final CatwalkModelUpdater catwalkModelUpdater;
    private final MiruStats stats;
    private final ObjectMapper mapper;

    public CatwalkModelEndpoints(@Context CatwalkModelService catwalkModelService,
        @Context CatwalkModelUpdater catwalkModelUpdater,
        @Context MiruStats stats,
        @Context ObjectMapper mapper) {
        this.catwalkModelService = catwalkModelService;
        this.catwalkModelUpdater = catwalkModelUpdater;
        this.stats = stats;
        this.mapper = mapper;
    }

    @POST
    @Path("/get/{tenantId}/{catwalkId}/{modelId}/{partitionId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response getModel(@PathParam("tenantId") String tenantId,
        @PathParam("catwalkId") String catwalkId,
        @PathParam("modelId") String modelId,
        @PathParam("partitionId") int partitionId,
        CatwalkQuery catwalkQuery) {
        long start = System.currentTimeMillis();
        try {
            MiruTenantId miruTenantId = new MiruTenantId(tenantId.getBytes(StandardCharsets.UTF_8));
            try {
                catwalkModelUpdater.updateModel(miruTenantId, catwalkId, modelId, partitionId, catwalkQuery);
            } catch (Exception x) {
                LOG.error("Failed to update model for {} {} {} {}", new Object[] { tenantId, catwalkId, modelId, partitionId }, x);
            }
            CatwalkModel model = catwalkModelService.getModel(miruTenantId, catwalkId, modelId, catwalkQuery);
            StreamingOutput stream = os -> {
                os.flush();
                BufferedOutputStream bos = new BufferedOutputStream(new SnappyOutputStream(os), 8192); // TODO expose to config
                try {
                    mapper.writeValue(bos, model);
                } finally {
                    bos.flush();
                }
            };
            long latency = System.currentTimeMillis() - start;
            stats.ingressed("/miru/catwalk/model/get/success", 1, latency);
            stats.ingressed("/miru/catwalk/model/get/" + tenantId + "/success", 1, latency);
            return Response.ok(stream).build();
        } catch (Exception e) {
            long latency = System.currentTimeMillis() - start;
            stats.ingressed("/miru/catwalk/model/get/failure", 1, latency);
            stats.ingressed("/miru/catwalk/model/get/" + tenantId + "/failure", 1, latency);
            LOG.error("Failed to get model for {} {} {}", new Object[] { tenantId, catwalkId, modelId }, e);
            return Response.serverError().entity("Failed to get model").build();
        }
    }

    @POST
    @Path("/update/{tenantId}/{catwalkId}/{modelId}/{partitionId}")
    @Produces(MediaType.TEXT_HTML)
    public Response updateModel(@PathParam("tenantId") String tenantId,
        @PathParam("catwalkId") String catwalkId,
        @PathParam("modelId") String modelId,
        @PathParam("partitionId") int partitionId,
        CatwalkQuery catwalkQuery) {
        long start = System.currentTimeMillis();
        try {
            catwalkModelUpdater.updateModel(new MiruTenantId(tenantId.getBytes(StandardCharsets.UTF_8)),
                catwalkId,
                modelId,
                partitionId,
                catwalkQuery);
            long latency = System.currentTimeMillis() - start;
            stats.ingressed("/miru/catwalk/model/update/success", 1, latency);
            stats.ingressed("/miru/catwalk/model/update/" + tenantId + "/success", 1, latency);
            return Response.ok("success").build();
        } catch (Exception e) {
            long latency = System.currentTimeMillis() - start;
            stats.ingressed("/miru/catwalk/model/update/failure", 1, latency);
            stats.ingressed("/miru/catwalk/model/update/" + tenantId + "/failure", 1, latency);
            LOG.error("Failed to update model for {} {} {} {}", new Object[] { tenantId, catwalkId, modelId, partitionId }, e);
            return Response.serverError().entity("Failed to update model").build();
        }
    }

}
