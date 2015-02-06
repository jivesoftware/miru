package com.jivesoftware.os.miru.manage.deployable.region;

import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.manage.deployable.MiruManageService;
import com.jivesoftware.os.miru.manage.deployable.region.RecoPluginRegion.RecoPluginRegionInput;
import java.util.List;
import javax.inject.Singleton;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 *
 */
@Singleton
@Path("/miru/manage/reco")
public class RecoPluginEndpoints {

    private final MiruManageService miruManageService;
    private final RecoPluginRegion recoPluginRegion;

    public RecoPluginEndpoints(@Context MiruManageService miruManageService, @Context RecoPluginRegion recoPluginRegion) {
        this.miruManageService = miruManageService;
        this.recoPluginRegion = recoPluginRegion;
    }

    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response getRecommendations(@QueryParam("tenantId") @DefaultValue("") String tenantId,
        @QueryParam("fromHoursAgo") @DefaultValue("144") int fromHoursAgo,
        @QueryParam("toHoursAgo") @DefaultValue("0") int toHoursAgo,
        @QueryParam("baseField") @DefaultValue("parent") String baseField,
        @QueryParam("contributorField") @DefaultValue("user") String contributorField,
        @QueryParam("recommendField") @DefaultValue("parent") String recommendField,
        @QueryParam("constraintsFilter") @DefaultValue("") String constraintsFilter,
        @QueryParam("scorableFilter") @DefaultValue("") String scorableFilter,
        @QueryParam("removeDistinctsFilter") @DefaultValue("") String removeDistinctsFilter,
        @QueryParam("removeDistinctsPrefixes") @DefaultValue("") String removeDistinctsPrefixesString,
        @QueryParam("logLevel") @DefaultValue("NONE") String logLevel) {

        List<String> removeDistinctsPrefixes = null;
        if (removeDistinctsPrefixesString != null && !removeDistinctsPrefixesString.isEmpty()) {
            removeDistinctsPrefixes = Lists.newArrayList(Splitter.onPattern("\\s*,\\s*").split(removeDistinctsPrefixesString));
            if (removeDistinctsPrefixes.isEmpty()) {
                removeDistinctsPrefixes = null;
            }
        }
        String rendered = miruManageService.renderPlugin(recoPluginRegion,
            Optional.of(new RecoPluginRegionInput(
                tenantId,
                fromHoursAgo,
                toHoursAgo,
                baseField,
                contributorField,
                recommendField,
                constraintsFilter,
                scorableFilter,
                removeDistinctsFilter,
                removeDistinctsPrefixes,
                logLevel)));
        return Response.ok(rendered).build();
    }
}
