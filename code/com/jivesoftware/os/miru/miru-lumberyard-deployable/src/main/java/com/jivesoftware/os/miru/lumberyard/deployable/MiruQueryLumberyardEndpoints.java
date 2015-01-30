package com.jivesoftware.os.miru.lumberyard.deployable;

import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 *
 */
@Singleton
@Path("/miru/lumberyard/query")
public class MiruQueryLumberyardEndpoints {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruQueryLumberyardService miruQueryLumberyardService;

    public MiruQueryLumberyardEndpoints(@Context MiruQueryLumberyardService lumberyardService) {
        this.miruQueryLumberyardService = lumberyardService;
    }

    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response get() {
        String rendered = miruQueryLumberyardService.render();
        return Response.ok(rendered).build();
    }

}
