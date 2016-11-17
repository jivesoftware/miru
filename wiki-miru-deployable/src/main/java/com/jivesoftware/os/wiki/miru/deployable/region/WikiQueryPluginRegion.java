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
import com.jivesoftware.os.wiki.miru.deployable.WikiMiruIndexService.Content;
import com.jivesoftware.os.wiki.miru.deployable.region.WikiQueryPluginRegion.WikiMiruPluginRegionInput;
import com.jivesoftware.os.wiki.miru.deployable.storage.WikiMiruPayloadsAmza;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    private final LuceneBackedQueryParser titleQueryParser = new LuceneBackedQueryParser("title");
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
                            "title",
                            locale,
                            query,
                            MiruFilter.NO_FILTER,
                            Strategy.TIME,
                            100,
                            new String[] { "userGuid", "folderGuid", "guid", "type" }),
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
                    List<String> contentKeys = Lists.newArrayList();

                    Set<String> uniqueFolders = new HashSet<>();
                    Set<String> uniqueUsers = new HashSet<>();

                    Map<String,Integer> foldersIndex = new HashMap<>();
                    Map<String,Integer> usersIndex = new HashMap<>();

                    List<String> folderKeys = Lists.newArrayList();
                    List<String> userKeys = Lists.newArrayList();

                    int folderIndex= 0;
                    int userIndex= 0;

                    for (ActivityScore score : scores) {
                        if (score.values[3][0].last().equals("content")) {
                            contentKeys.add(score.values[2][0].last() + "-slug");
                        } else {
                            contentKeys.add(score.values[2][0].last());
                        }
                        if (score.values[0] != null && score.values[0].length > 0) {

                            String userGuid = score.values[0][0].last();
                            if (uniqueUsers.add(userGuid)) {
                                userKeys.add(userGuid);
                                usersIndex.put(userGuid,userIndex);
                                userIndex++;
                            }
                        }

                        if (score.values[1] != null && score.values[1].length > 0) {
                            String folderGuid = score.values[1][0].last();
                            if (uniqueFolders.add(folderGuid)) {
                                folderKeys.add(folderGuid);
                                foldersIndex.put(folderGuid,folderIndex);
                                folderIndex++;
                            }
                        }
                    }

                    long start = System.currentTimeMillis();
                    List<Content> usersContent = payloads.multiGet(tenantId, userKeys, Content.class);
                    long elapsed = System.currentTimeMillis() - start;
                    data.put("getUsersElapse", String.valueOf(elapsed));

                    start = System.currentTimeMillis();
                    List<Content> foldersContent = payloads.multiGet(tenantId, folderKeys, Content.class);
                    elapsed = System.currentTimeMillis() - start;
                    data.put("getFoldersElapse", String.valueOf(elapsed));

                    start = System.currentTimeMillis();
                    List<Content> contents = payloads.multiGet(tenantId, contentKeys, Content.class);
                    elapsed = System.currentTimeMillis() - start;
                    data.put("getContentElapse", String.valueOf(elapsed));

                    List<Map<String, Object>> results = new ArrayList<>();

                    start = System.currentTimeMillis();
                    int i = 0;
                    for (ActivityScore score : scores) {
                        Content content = contents.get(i);
                        if (content != null) {

                            Map<String, Object> result = new HashMap<>();
                            result.put("type", score.values[3][0].last());

                            if (score.values[0] != null && score.values[0].length > 0) {

                                String userGuid = score.values[0][0].last();
                                Content userContent = usersContent.get(usersIndex.get(userGuid));
                                if (userContent != null) {
                                    result.put("userGuid", userGuid);
                                    result.put("user", userContent.title);
                                }
                            }

                            if (score.values[1] != null && score.values[1].length > 0) {
                                String folderGuid = score.values[1][0].last();
                                Content folderContent = foldersContent.get(foldersIndex.get(folderGuid));
                                if (folderContent != null) {
                                    result.put("folderGuid", folderGuid);
                                    result.put("folder", folderContent.title);
                                }
                            }


                            result.put("guid", score.values[2][0].last());
                            result.put("title", content.title);
                            result.put("body",
                                bodyQueryParser.highlight(locale, input.query, content.body, "<span style=\"background-color: #FFFF00\">", "</span>", 1000));
                            results.add(result);
                        }
                        i++;
                    }
                    elapsed = System.currentTimeMillis() - start;
                    data.put("highlightElapse", String.valueOf(elapsed));
                    data.put("results", results);

                    List<Map<String, Object>> users = new ArrayList<>();
                    i = 0;
                    for (String userKey : userKeys) {
                        Content content = usersContent.get(i);
                        Map<String, Object> result = new HashMap<>();
                        result.put("guid", userKey);
                        result.put("title", content.title);
                        result.put("body",
                            bodyQueryParser.highlight(locale, input.query, content.body, "<span style=\"background-color: #FFFF00\">", "</span>", 1000));
                        results.add(result);
                        i++;
                    }
                    data.put("users", users);

                    List<Map<String, Object>> folders = new ArrayList<>();
                    i = 0;
                    for (String folderKey : folderKeys) {
                        Content content = foldersContent.get(i);

                        Map<String, Object> result = new HashMap<>();
                        result.put("guid", folderKey);
                        result.put("title", content.title);
                        result.put("body",
                            bodyQueryParser.highlight(locale, input.query, content.body, "<span style=\"background-color: #FFFF00\">", "</span>", 1000));
                        results.add(result);
                        i++;
                    }
                    data.put("folders", users);


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
            part[i] = "( title:" + part[i] + " OR body:" + part[i] + ")";
        }
        return Joiner.on(" AND ").join(part);
    }

    @Override
    public String getTitle() {
        return "Wiki Query";
    }
}
