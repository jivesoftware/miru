package com.jivesoftware.os.wiki.miru.deployable.region;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.api.MiruActorId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.plugin.query.LuceneBackedQueryParser;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.stream.plugins.fulltext.FullTextAnswer;
import com.jivesoftware.os.miru.stream.plugins.fulltext.FullTextAnswer.ActivityScore;
import com.jivesoftware.os.miru.stream.plugins.fulltext.FullTextConstants;
import com.jivesoftware.os.miru.stream.plugins.fulltext.FullTextQuery;
import com.jivesoftware.os.miru.stream.plugins.fulltext.FullTextQuery.Strategy;
import com.jivesoftware.os.miru.ui.MiruPageRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.http.client.HttpResponse;
import com.jivesoftware.os.routing.bird.http.client.HttpResponseMapper;
import com.jivesoftware.os.routing.bird.http.client.RoundRobinStrategy;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.routing.bird.shared.ClientCall.ClientResponse;
import com.jivesoftware.os.wiki.miru.deployable.WikiMiruIndexService.Wiki;
import com.jivesoftware.os.wiki.miru.deployable.region.WikiQueryPluginRegion.WikiMiruPluginRegionInput;
import com.jivesoftware.os.wiki.miru.deployable.storage.WikiMiruPayloadsAmza;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class WikiQueryPluginRegion implements MiruPageRegion<WikiMiruPluginRegionInput> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final String template;
    private final MiruSoyRenderer renderer;
    private final TenantAwareHttpClient<String> readerClient;
    private final ObjectMapper requestMapper;
    private final HttpResponseMapper responseMapper;
    private final WikiMiruPayloadsAmza payloads;

    private final LuceneBackedQueryParser subjectQueryParser = new LuceneBackedQueryParser("subject");
    private final LuceneBackedQueryParser bodyQueryParser = new LuceneBackedQueryParser("body");

    public WikiQueryPluginRegion(String template,
        MiruSoyRenderer renderer,
        TenantAwareHttpClient<String> readerClient,
        ObjectMapper requestMapper,
        HttpResponseMapper responseMapper,
        WikiMiruPayloadsAmza payloads) {

        this.template = template;
        this.renderer = renderer;
        this.readerClient = readerClient;
        this.requestMapper = requestMapper;
        this.responseMapper = responseMapper;
        this.payloads = payloads;
    }

    public static class WikiMiruPluginRegionInput {

        final String tenantId;
        final String query;

        public WikiMiruPluginRegionInput(String tenantId, String query) {
            this.tenantId = tenantId;
            this.query = query;
        }
    }

    @Override
    public String render(WikiMiruPluginRegionInput input) {
        Map<String, Object> data = Maps.newHashMap();
        try {

            data.put("tenantId", input.tenantId);
            data.put("query", input.query);


            String query = rewrite(input.query.toLowerCase());


            if (!input.tenantId.trim().isEmpty()) {

                MiruTenantId tenantId = new MiruTenantId(input.tenantId.trim().getBytes(Charsets.UTF_8));
                String endpoint = FullTextConstants.FULLTEXT_PREFIX + FullTextConstants.CUSTOM_QUERY_ENDPOINT;
                String locale = "en";
                String request = requestMapper.writeValueAsString(
                    new MiruRequest<>(
                        "wiki-miru",
                        tenantId,
                        MiruActorId.NOT_PROVIDED,
                        MiruAuthzExpression.NOT_PROVIDED,
                        new FullTextQuery(
                            MiruTimeRange.ALL_TIME,
                            "subject",
                            locale,
                            query,
                            MiruFilter.NO_FILTER,
                            Strategy.TIME,
                            100,
                            new String[] { "id" }),
                        MiruSolutionLogLevel.NONE)
                );


                MiruResponse<FullTextAnswer> response = readerClient.call("",
                    new RoundRobinStrategy(),
                    "wikiQueryPluginRegion",
                    httpClient -> {
                        HttpResponse httpResponse = httpClient.postJson(endpoint, request, null);
                        @SuppressWarnings("unchecked")
                        MiruResponse<FullTextAnswer> extractResponse = responseMapper.extractResultFromResponse(httpResponse,
                            MiruResponse.class,
                            new Class[] { FullTextAnswer.class },
                            null);
                        return new ClientResponse<>(extractResponse, true);
                    });

                if (response != null && response.answer != null) {
                    data.put("elapse", String.valueOf(response.totalElapsed));
                    data.put("count", response.answer.results.size());
                    List<ActivityScore> scores = response.answer.results.subList(0, Math.min(1_000, response.answer.results.size()));
                    List<Map<String, Object>> results = new ArrayList<>();
                    List<String> keys = Lists.newArrayList();
                    for (ActivityScore score : scores) {
                        keys.add(score.values[0][0].last()+"-slug");
                    }

                    long start = System.currentTimeMillis();
                    List<Wiki> wikis = payloads.multiGet(tenantId, keys, Wiki.class);
                    long elapsed = System.currentTimeMillis() - start;
                    data.put("getElapse", String.valueOf(elapsed));

                    start = System.currentTimeMillis();
                    for (int i = 0; i < keys.size(); i++) {
                        Wiki wiki = wikis.get(i);
                        if (wiki != null) {

                            Map<String, Object> result = new HashMap<>();
                            result.put("id", keys.get(i));
                            result.put("subject", wiki.subject);
                            result.put("body",
                                bodyQueryParser.highlight(locale, input.query, wiki.body, "<span style=\"background-color: #FFFF00\">", "</span>", 1000));
                            results.add(result);
                        }
                    }
                    elapsed = System.currentTimeMillis() - start;
                    data.put("highlightElapse", String.valueOf(elapsed));
                    data.put("results", results);

                    ObjectMapper mapper = new ObjectMapper();
                    mapper.enable(SerializationFeature.INDENT_OUTPUT);
                    data.put("summary", Joiner.on("\n").join(response.log) + "\n\n" + mapper.writeValueAsString(response.solutions));
                } else {
                    LOG.warn("Empty full text response from {}", tenantId);
                }
            }


        } catch (Exception e) {
            LOG.error("Unable to retrieve data", e);
        }
        return renderer.render(template, data);
    }

    private String rewrite(String query) {
        String[] part = query.split("\\s+");
        if (part.length > 0 && !part[part.length - 1].endsWith("*")) {
            part[part.length - 1] += "*";
        }
        for (int i = 0; i < part.length; i++) {
            part[i] = "( subject:" + part[i] + " OR body:" + part[i] + ")";
        }
        return Joiner.on(" AND ").join(part);
    }

    @Override
    public String getTitle() {
        return "Wiki Query";
    }
}
