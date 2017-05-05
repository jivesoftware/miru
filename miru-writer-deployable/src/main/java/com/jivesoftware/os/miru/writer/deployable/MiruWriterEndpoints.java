package com.jivesoftware.os.miru.writer.deployable;

import javax.inject.Singleton;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 *
 */
@Singleton
@Path("/ui")
public class MiruWriterEndpoints {

    private final MiruWriterUIService writerUIService;

    public MiruWriterEndpoints(@Context MiruWriterUIService writerUIService) {
        this.writerUIService = writerUIService;
    }

    @GET
    @Produces(MediaType.TEXT_HTML)
    public Response get(@HeaderParam("rb_session_redir_url") @DefaultValue("") String redirUrl) {
        String rendered = writerUIService.render(redirUrl);
        return Response.ok(rendered).build();
    }

}
