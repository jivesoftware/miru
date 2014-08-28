package com.jivesoftware.os.miru.service.endpoint;

import com.jivesoftware.os.jive.utils.jaxrs.util.ResponseHelper;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.service.MiruService;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static com.jivesoftware.os.miru.api.MiruReader.QUERY_SERVICE_ENDPOINT_PREFIX;
import static com.jivesoftware.os.miru.api.MiruReader.WARM_ENDPOINT;

@Path(QUERY_SERVICE_ENDPOINT_PREFIX)
public class MiruReaderEndpoints {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final MiruService miruService;
    private final ResponseHelper responseHelper = ResponseHelper.INSTANCE;

    public MiruReaderEndpoints(@Context MiruService miruService) {
        this.miruService = miruService;
    }

    @POST
    @Path(WARM_ENDPOINT)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response warm(MiruTenantId tenantId) {
        try {
            miruService.warm(tenantId);
            return responseHelper.jsonResponse("");
        } catch (Exception e) {
            log.error("Failed to warm.", e);
            return Response.serverError().build();
        }
    }

}
