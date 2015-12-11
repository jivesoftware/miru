package com.jivesoftware.os.miru.tools.deployable.region;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.miru.api.MiruActorId;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.filter.FilterStringUtil;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.topology.ReaderRequestHelpers;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsQuery;
import com.jivesoftware.os.miru.reco.plugins.reco.RecoAnswer;
import com.jivesoftware.os.miru.reco.plugins.reco.RecoAnswer.Recommendation;
import com.jivesoftware.os.miru.reco.plugins.reco.RecoConstants;
import com.jivesoftware.os.miru.reco.plugins.reco.RecoQuery;
import com.jivesoftware.os.miru.ui.MiruPageRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.http.client.HttpRequestHelper;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
// soy.miru.page.recoPluginRegion
public class RecoPluginRegion implements MiruPageRegion<Optional<RecoPluginRegion.RecoPluginRegionInput>> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final String template;
    private final MiruSoyRenderer renderer;
    private final ReaderRequestHelpers readerRequestHelpers;
    private final FilterStringUtil filterStringUtil = new FilterStringUtil();

    public RecoPluginRegion(String template,
        MiruSoyRenderer renderer,
        ReaderRequestHelpers readerRequestHelpers) {
        this.template = template;
        this.renderer = renderer;
        this.readerRequestHelpers = readerRequestHelpers;
    }

    public static class RecoPluginRegionInput {

        final String tenant;
        final int fromHoursAgo;
        final int toHoursAgo;
        final String baseField;
        final String contributorField;
        final String recommendField;
        final String constraintsFilter;
        final String scorableFilter;
        final String removeDistinctsFilter;
        final List<String> removeDistinctsPrefixes;
        final String logLevel;

        public RecoPluginRegionInput(String tenant,
            int fromHoursAgo,
            int toHoursAgo,
            String baseField,
            String contributorField,
            String recommendField,
            String constraintsFilter,
            String scorableFilter,
            String removeDistinctsFilter,
            List<String> removeDistinctsPrefixes,
            String logLevel) {

            this.tenant = tenant;
            this.fromHoursAgo = fromHoursAgo;
            this.toHoursAgo = toHoursAgo;
            this.baseField = baseField;
            this.contributorField = contributorField;
            this.recommendField = recommendField;
            this.constraintsFilter = constraintsFilter;
            this.scorableFilter = scorableFilter;
            this.removeDistinctsFilter = removeDistinctsFilter;
            this.removeDistinctsPrefixes = removeDistinctsPrefixes;
            this.logLevel = logLevel;
        }
    }

    @Override
    public String render(Optional<RecoPluginRegionInput> optionalInput) {
        Map<String, Object> data = Maps.newHashMap();
        try {
            if (optionalInput.isPresent()) {
                RecoPluginRegionInput input = optionalInput.get();
                int fromHoursAgo = input.fromHoursAgo > input.toHoursAgo ? input.fromHoursAgo : input.toHoursAgo;
                int toHoursAgo = input.fromHoursAgo > input.toHoursAgo ? input.toHoursAgo : input.fromHoursAgo;

                data.put("logLevel", input.logLevel);
                data.put("tenant", input.tenant);
                data.put("fromHoursAgo", String.valueOf(fromHoursAgo));
                data.put("toHoursAgo", String.valueOf(toHoursAgo));
                data.put("baseField", input.baseField);
                data.put("contributorField", input.contributorField);
                data.put("recommendField", input.recommendField);
                data.put("constraintsFilter", input.constraintsFilter);
                data.put("scorableFilter", input.scorableFilter);
                data.put("removeDistinctsFilter", input.removeDistinctsFilter);
                data.put("removeDistinctsPrefixes", input.removeDistinctsPrefixes != null ? Joiner.on(", ").join(input.removeDistinctsPrefixes) : "");

                SnowflakeIdPacker snowflakeIdPacker = new SnowflakeIdPacker();
                long jiveCurrentTime = new JiveEpochTimestampProvider().getTimestamp();
                final long packCurrentTime = snowflakeIdPacker.pack(jiveCurrentTime, 0, 0);
                final long fromTime = packCurrentTime - snowflakeIdPacker.pack(TimeUnit.HOURS.toMillis(fromHoursAgo), 0, 0);
                final long toTime = packCurrentTime - snowflakeIdPacker.pack(TimeUnit.HOURS.toMillis(toHoursAgo), 0, 0);
                MiruTimeRange timeRange = new MiruTimeRange(fromTime, toTime);

                DistinctsQuery removeDistinctsQuery = null;
                if (input.removeDistinctsFilter != null && !input.removeDistinctsFilter.isEmpty() ||
                    input.removeDistinctsPrefixes != null && !input.removeDistinctsPrefixes.isEmpty()) {

                    removeDistinctsQuery = new DistinctsQuery(
                        timeRange,
                        input.recommendField,
                        null,
                        filterStringUtil.parse(input.removeDistinctsFilter),
                        filterStringUtil.buildFieldPrefixes(input.removeDistinctsPrefixes));
                }

                MiruFilter constraintsFilter = filterStringUtil.parse(input.constraintsFilter);
                MiruFilter scorableFilter = filterStringUtil.parse(input.scorableFilter);

                List<HttpRequestHelper> requestHelpers = readerRequestHelpers.get(Optional.<MiruHost>absent());
                MiruResponse<RecoAnswer> response = null;
                if (!input.tenant.trim().isEmpty()) {
                    MiruTenantId tenantId = new MiruTenantId(input.tenant.trim().getBytes(Charsets.UTF_8));
                    for (HttpRequestHelper requestHelper : requestHelpers) {
                        try {
                            @SuppressWarnings("unchecked")
                            MiruResponse<RecoAnswer> recoResponse = requestHelper.executeRequest(
                                new MiruRequest<>("toolsReco",
                                    tenantId,
                                    MiruActorId.NOT_PROVIDED,
                                    MiruAuthzExpression.NOT_PROVIDED,
                                    new RecoQuery(
                                        timeRange,
                                        removeDistinctsQuery,
                                        constraintsFilter,
                                        input.baseField,
                                        input.contributorField,
                                        input.recommendField,
                                        scorableFilter,
                                        100),
                                    MiruSolutionLogLevel.valueOf(input.logLevel)),
                                RecoConstants.RECO_PREFIX + RecoConstants.CUSTOM_QUERY_ENDPOINT, MiruResponse.class,
                                new Class[] { RecoAnswer.class },
                                null);
                            response = recoResponse;
                            if (response != null && response.answer != null) {
                                break;
                            } else {
                                log.warn("Empty reco response from {}, trying another", requestHelper);
                            }
                        } catch (Exception e) {
                            log.warn("Failed reco request to {}, trying another", new Object[] { requestHelper }, e);
                        }
                    }
                }

                if (response != null && response.answer != null) {
                    data.put("elapse", String.valueOf(response.totalElapsed));

                    List<Recommendation> results = response.answer.results;
                    if (results == null) {
                        results = Collections.emptyList();
                    }
                    data.put("elapse", String.valueOf(response.totalElapsed));

                    data.put("results", Lists.transform(results, recommendation -> ImmutableMap.of(
                        "name", recommendation.distinctValue.last(),
                        "rank", String.valueOf(recommendation.rank))));
                    ObjectMapper mapper = new ObjectMapper();
                    mapper.enable(SerializationFeature.INDENT_OUTPUT);
                    data.put("summary", Joiner.on("\n").join(response.log) + "\n\n" + mapper.writeValueAsString(response.solutions));
                }
            }
        } catch (Exception e) {
            log.error("Unable to retrieve data", e);
        }

        return renderer.render(template, data);
    }

    @Override
    public String getTitle() {
        return "Reco";
    }
}
