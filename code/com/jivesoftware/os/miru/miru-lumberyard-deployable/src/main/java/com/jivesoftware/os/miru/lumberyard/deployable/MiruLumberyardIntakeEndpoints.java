package com.jivesoftware.os.miru.lumberyard.deployable;

import com.jivesoftware.os.miru.lumberyard.deployable.MiruLumberyardIntakeService.MiruLogEvent;
import java.util.List;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 *
 */
@Singleton
@Path("/miru/lumberyard")
public class MiruLumberyardIntakeEndpoints {

    private final MiruLumberyardIntakeService intakeService;

    public MiruLumberyardIntakeEndpoints(@Context MiruLumberyardIntakeService intakeService) {
        this.intakeService = intakeService;
    }

    @POST
    @Path("intake")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_HTML)
    public Response intake(List<MiruLogEvent> logEvents) throws Exception {
        intakeService.ingressLogEvents(logEvents);
        return Response.ok().build();
    }

}
