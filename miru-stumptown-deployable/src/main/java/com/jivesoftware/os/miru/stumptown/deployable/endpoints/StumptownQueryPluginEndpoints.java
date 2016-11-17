package com.jivesoftware.os.miru.stumptown.deployable.endpoints;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.jivesoftware.os.miru.stumptown.deployable.MiruStumptownService;
import com.jivesoftware.os.miru.stumptown.deployable.region.StumptownQueryPluginRegion;
import com.jivesoftware.os.miru.stumptown.deployable.region.StumptownQueryPluginRegion.StumptownPluginRegionInput;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.shared.ResponseHelper;
import java.util.List;
import java.util.Map;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 *
 */
@Singleton
@Path("/ui/query")
public class StumptownQueryPluginEndpoints {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruStumptownService stumptownService;
    private final StumptownQueryPluginRegion pluginRegion;

    private final ResponseHelper responseHelper = ResponseHelper.INSTANCE;

    public StumptownQueryPluginEndpoints(@Context MiruStumptownService stumptownService, @Context StumptownQueryPluginRegion pluginRegion) {
        this.stumptownService = stumptownService;
        this.pluginRegion = pluginRegion;
    }

    @GET
    @Produces(MediaType.TEXT_HTML)
    public Response query(
        @QueryParam("cluster") @DefaultValue("") String cluster,
        @QueryParam("host") @DefaultValue("") String host,
        @QueryParam("service") @DefaultValue("") String service,
        @QueryParam("instance") @DefaultValue("") String instance,
        @QueryParam("version") @DefaultValue("") String version,
        @QueryParam("logLevels") @DefaultValue("INFO,WARN,DEBUG,TRACE,ERROR") List<String> logLevels,
        @QueryParam("fromAgo") @DefaultValue("1") int fromAgo,
        @QueryParam("toAgo") @DefaultValue("0") int toAgo,
        @QueryParam("fromTimeUnit") @DefaultValue("HOURS") String fromTimeUnit,
        @QueryParam("toTimeUnit") @DefaultValue("HOURS") String toTimeUnit,
        @QueryParam("thread") @DefaultValue("") String thread,
        @QueryParam("logger") @DefaultValue("") String logger,
        @QueryParam("method") @DefaultValue("") String method,
        @QueryParam("line") @DefaultValue("") String line,
        @QueryParam("message") @DefaultValue("") String message,
        @QueryParam("exceptionClass") @DefaultValue("") String exceptionClass,
        @QueryParam("thrown") @DefaultValue("") String thrown,
        @QueryParam("buckets") @DefaultValue("30") int buckets,
        @QueryParam("messageCount") @DefaultValue("100") int messageCount,
        @QueryParam("graphType") @DefaultValue("Line") String graphType) {
        String rendered = stumptownService.renderPlugin(pluginRegion,
            Optional.of(new StumptownPluginRegionInput(cluster,
                host,
                service,
                instance,
                version,
                Joiner.on(',').join(logLevels),
                fromAgo,
                toAgo,
                fromTimeUnit,
                toTimeUnit,
                thread,
                logger,
                method,
                line,
                message,
                exceptionClass,
                thrown,
                buckets,
                messageCount,
                graphType)));
        return Response.ok(rendered).build();
    }

    @POST
    @Path("/poll")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public Response poll(
        @FormParam("cluster") @DefaultValue("dev") String cluster,
        @FormParam("host") @DefaultValue("") String host,
        @FormParam("service") @DefaultValue("") String service,
        @FormParam("instance") @DefaultValue("") String instance,
        @FormParam("version") @DefaultValue("") String version,
        @FormParam("logLevels") @DefaultValue("INFO,WARN,DEBUG,TRACE,ERROR") List<String> logLevels,
        @FormParam("fromAgo") @DefaultValue("1") int fromAgo,
        @FormParam("toAgo") @DefaultValue("0") int toAgo,
        @FormParam("fromTimeUnit") @DefaultValue("HOURS") String fromTimeUnit,
        @FormParam("toTimeUnit") @DefaultValue("HOURS") String toTimeUnit,
        @FormParam("thread") @DefaultValue("") String thread,
        @FormParam("logger") @DefaultValue("") String logger,
        @FormParam("method") @DefaultValue("") String method,
        @FormParam("line") @DefaultValue("") String line,
        @FormParam("message") @DefaultValue("") String message,
        @FormParam("exceptionClass") @DefaultValue("") String exceptionClass,
        @FormParam("thrown") @DefaultValue("") String thrown,
        @FormParam("buckets") @DefaultValue("30") int buckets,
        @FormParam("messageCount") @DefaultValue("100") int messageCount) {
        try {
            Map<String, Object> result = pluginRegion.poll(new StumptownPluginRegionInput(cluster,
                host,
                service,
                instance,
                version,
                Joiner.on(',').join(logLevels),
                fromAgo,
                toAgo,
                fromTimeUnit,
                toTimeUnit,
                thread,
                logger,
                method,
                line,
                message,
                exceptionClass,
                thrown,
                buckets,
                messageCount,
                null));
            return responseHelper.jsonResponse(result != null ? result : "");
        } catch (Exception e) {
            LOG.error("Stumptown poll failed", e);
            return responseHelper.errorResponse("Stumptown poll failed", e);
        }
    }


    @GET
    @Path("/typeahead/{fieldName}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response typeahead(
        @PathParam("fieldName") @DefaultValue("") String fieldName,
        @QueryParam("cluster") @DefaultValue("dev") String cluster,
        @QueryParam("host") @DefaultValue("") String host,
        @QueryParam("service") @DefaultValue("") String service,
        @QueryParam("instance") @DefaultValue("") String instance,
        @QueryParam("version") @DefaultValue("") String version,
        @QueryParam("fromAgo") @DefaultValue("1") int fromAgo,
        @QueryParam("toAgo") @DefaultValue("0") int toAgo,
        @QueryParam("fromTimeUnit") @DefaultValue("HOURS") String fromTimeUnit,
        @QueryParam("toTimeUnit") @DefaultValue("HOURS") String toTimeUnit,
        @QueryParam("thread") @DefaultValue("") String thread,
        @QueryParam("logger") @DefaultValue("") String logger,
        @QueryParam("method") @DefaultValue("") String method,
        @QueryParam("line") @DefaultValue("") String line,
        @QueryParam("contains") @DefaultValue("") String contains) {
        try {
            List<Map<String, String>> data = pluginRegion.typeahead(fieldName, cluster, host, service, instance, version, fromAgo, toAgo, fromTimeUnit,
                toTimeUnit, thread, logger, method, line, contains);
            return Response.ok(new ObjectMapper().writeValueAsString(data)).build();
        } catch (Exception x) {
            LOG.error("Failed to generating query ui.", x);
            return Response.serverError().build();
        }
    }

}
