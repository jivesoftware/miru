package com.jivesoftware.os.wiki.miru.deployable.region;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.api.MiruActorId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFieldFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilterOperation;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
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
        final String folderGuids;
        final String userGuids;

        public WikiMiruPluginRegionInput(String tenantId, String query, String folderIds, String userIds) {
            this.tenantId = tenantId;
            this.query = query;
            this.folderGuids = folderIds;
            this.userGuids = userIds;
        }
    }

    @Override
    public String render(WikiMiruPluginRegionInput input) {
        Map<String, Object> data = Maps.newHashMap();
        try {

            data.put("tenantId", input.tenantId);
            data.put("query", input.query);


            String query = input.query.isEmpty() ? "" : rewrite(input.query.toLowerCase());


            if (!input.tenantId.trim().isEmpty()) {

                MiruTenantId tenantId = new MiruTenantId(input.tenantId.trim().getBytes(Charsets.UTF_8));


                List<MiruFieldFilter> ands = new ArrayList<>();
                ands.add(MiruFieldFilter.of(MiruFieldType.primary, "type", Arrays.asList("content")));

                if (!input.userGuids.isEmpty()) {
                    ands.add(MiruFieldFilter.of(MiruFieldType.primary, "userGuid", Arrays.asList(input.userGuids)));
                }

                if (!input.folderGuids.isEmpty()) {
                    ands.add(MiruFieldFilter.of(MiruFieldType.primary, "folderGuid", Arrays.asList(input.folderGuids)));
                }

                MiruFilter contentsFilter = new MiruFilter(MiruFilterOperation.and, false, ands, null);


                MiruResponse<FullTextAnswer> response = query(tenantId, contentsFilter, query);

                List<String> contentKeys = Lists.newArrayList();

                Set<String> uniqueFolders = new HashSet<>();
                Set<String> uniqueUsers = new HashSet<>();

                Map<String, Integer> foldersIndex = new HashMap<>();
                Map<String, Integer> usersIndex = new HashMap<>();

                List<String> folderKeys = Lists.newArrayList();
                List<String> userKeys = Lists.newArrayList();

                if (response != null && response.answer != null) {
                    LOG.info("Found:{} for {}:{}", response.answer.results.size(), input.tenantId, query);

                    data.put("elapse", String.valueOf(response.totalElapsed));
                    data.put("count", String.valueOf(response.answer.results.size()));
                    data.put("found", String.valueOf(response.answer.found));
                    List<ActivityScore> scores = response.answer.results;


                    int folderIndex = 0;
                    int userIndex = 0;

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
                                usersIndex.put(userGuid, userIndex);
                                userIndex++;
                            }
                        }

                        if (score.values[1] != null && score.values[1].length > 0) {
                            String folderGuid = score.values[1][0].last();
                            if (uniqueFolders.add(folderGuid)) {
                                folderKeys.add(folderGuid);
                                foldersIndex.put(folderGuid, folderIndex);
                                folderIndex++;
                            }
                        }
                    }

                    ObjectMapper mapper = new ObjectMapper();
                    mapper.enable(SerializationFeature.INDENT_OUTPUT);
                    data.put("summary", Joiner.on("\n").join(response.log) + "\n\n" + mapper.writeValueAsString(response.solutions));
                } else {
                    LOG.warn("Empty full text response from {}", tenantId);
                }


                int contentKeysCount = contentKeys.size();
                int userKeysCount = userKeys.size();
                int folderKeysCount = folderKeys.size();

                List<String> allKeys = new ArrayList<>();
                allKeys.addAll(contentKeys);
                allKeys.addAll(userKeys);
                allKeys.addAll(folderKeys);

                if (!input.userGuids.isEmpty()) {
                    allKeys.add(input.userGuids);
                }
                if (!input.folderGuids.isEmpty()) {
                    allKeys.add(input.folderGuids);
                }


                long start = System.currentTimeMillis();
                List<Content> contents = payloads.multiGet(tenantId, allKeys, Content.class);
                long elapsed = System.currentTimeMillis() - start;
                data.put("getContentElapse", String.valueOf(elapsed));


                if (response != null && response.answer != null) {
                    List<Map<String, Object>> results = new ArrayList<>();
                    List<ActivityScore> scores = response.answer.results;

                    start = System.currentTimeMillis();
                    int i = 0;
                    for (ActivityScore score : scores) {
                        Content content = contents.get(i);
                        if (content != null) {

                            Map<String, Object> result = new HashMap<>();
                            result.put("type", score.values[3][0].last());

                            if (score.values[0] != null && score.values[0].length > 0) {

                                String userGuid = score.values[0][0].last();
                                Content userContent = contents.get(contentKeysCount + usersIndex.get(userGuid));
                                if (userContent != null) {
                                    result.put("userGuid", userGuid);
                                    result.put("user", userContent.title);
                                }
                            }

                            if (score.values[1] != null && score.values[1].length > 0) {
                                String folderGuid = score.values[1][0].last();
                                Content folderContent = contents.get(contentKeysCount + userKeysCount + foldersIndex.get(folderGuid));
                                if (folderContent != null) {
                                    result.put("folderGuid", folderGuid);
                                    result.put("folder", folderContent.title);
                                }
                            }


                            result.put("guid", score.values[2][0].last());
                            result.put("title", content.title);
                            result.put("body",
                                bodyQueryParser.highlight("en", input.query, content.body, "<span style=\"background-color: #FFFF00\">", "</span>", 1000));
                            results.add(result);
                        }
                        i++;
                    }

                    elapsed = System.currentTimeMillis() - start;
                    data.put("highlightElapse", String.valueOf(elapsed));
                    data.put("results", results);
                }

                int u = 0;
                if (!input.userGuids.isEmpty()) {
                    Content content = contents.get(contentKeysCount + userKeysCount + folderKeysCount + u);
                    Map<String, Object> result = new HashMap<>();

                    Random rand = new Random(input.userGuids.hashCode());
                    String gender = rand.nextDouble() > 0.5 ? "men" : "women";
                    int id = rand.nextInt(100);

                    String avatarUrl = "https://randomuser.me/api/portraits/" + gender + "/" + id + ".jpg";
                    result.put("avatarUrl", avatarUrl);
                    result.put("guid", input.userGuids);
                    result.put("title", content.title);
                    result.put("body",
                        bodyQueryParser.highlight("en", input.query, content.body, "<span style=\"background-color: #FFFF00\">", "</span>", 1000));

                    data.put("queryUsers", result);
                    u++;
                }
                if (!input.folderGuids.isEmpty()) {
                    Content content = contents.get(contentKeysCount + userKeysCount + folderKeysCount + u);
                    Map<String, Object> result = new HashMap<>();

                    Random rand = new Random(input.folderGuids.hashCode());

                    String folderUrl = "https://unsplash.it/200/300?image=" + rand.nextInt(1000);

                    result.put("folderUrl", folderUrl);
                    result.put("guid", input.folderGuids);
                    result.put("title", content.title);
                    result.put("body",
                        bodyQueryParser.highlight("en", input.query, content.body, "<span style=\"background-color: #FFFF00\">", "</span>", 1000));

                    data.put("queryFolders", result);
                }


                if (input.folderGuids.isEmpty() && input.userGuids.isEmpty()) {

                    MiruFilter usersFilter = new MiruFilter(MiruFilterOperation.and, false, Arrays.asList(MiruFieldFilter.of(MiruFieldType.primary, "type",
                        Arrays.asList("user"))), null);

                    MiruFilter foldersFilter = new MiruFilter(MiruFilterOperation.and, false, Arrays.asList(MiruFieldFilter.of(MiruFieldType.primary, "type",
                        Arrays.asList("folder"))), null);


                    start = System.currentTimeMillis();
                    MiruResponse<FullTextAnswer> users = query(tenantId, usersFilter, query);
                    elapsed = System.currentTimeMillis() - start;
                    data.put("usersElapse", String.valueOf(elapsed));

                    if (users != null && users.answer != null) {
                        data.put("usersCount", String.valueOf(users.answer.results.size()));
                        data.put("usersFound", String.valueOf((users.answer.found)));


                        List<String> keys = Lists.newArrayList();
                        for (ActivityScore score : users.answer.results) {
                            keys.add(score.values[2][0].last());
                        }

                        contents = payloads.multiGet(tenantId, keys, Content.class);

                        List<Map<String, Object>> results = new ArrayList<>();
                        int i = 0;

                        for (String key : keys) {
                            Content content = contents.get(i);
                            Map<String, Object> result = new HashMap<>();

                            Random rand = new Random(key.hashCode());
                            String gender = rand.nextDouble() > 0.5 ? "men" : "women";
                            int id = rand.nextInt(100);

                            String avatarUrl = "https://randomuser.me/api/portraits/" + gender + "/" + id + ".jpg";
                            result.put("avatarUrl", avatarUrl);
                            result.put("guid", key);
                            result.put("title", content.title);
                            result.put("body",
                                bodyQueryParser.highlight("en", input.query, content.body, "<span style=\"background-color: #FFFF00\">", "</span>", 1000));
                            results.add(result);
                            i++;
                        }
                        data.put("users", results);
                    }

                    start = System.currentTimeMillis();
                    MiruResponse<FullTextAnswer> folders = query(tenantId, foldersFilter, query);
                    elapsed = System.currentTimeMillis() - start;
                    data.put("foldersElapse", String.valueOf(elapsed));

                    if (folders != null && folders.answer != null) {
                        data.put("foldersCount", String.valueOf(folders.answer.results.size()));
                        data.put("foldersFound", String.valueOf((folders.answer.found)));

                        List<String> keys = Lists.newArrayList();
                        for (ActivityScore score : folders.answer.results) {
                            keys.add(score.values[2][0].last());
                        }

                        contents = payloads.multiGet(tenantId, keys, Content.class);

                        List<Map<String, Object>> results = new ArrayList<>();
                        int i = 0;
                        for (String key : keys) {
                            Content content = contents.get(i);
                            Map<String, Object> result = new HashMap<>();

                            Random rand = new Random(key.hashCode());

                            String folderUrl = "https://unsplash.it/200/300?image=" + rand.nextInt(1000);

                            result.put("folderUrl", folderUrl);
                            result.put("guid", key);
                            result.put("title", content.title);
                            result.put("body",
                                bodyQueryParser.highlight("en", input.query, content.body, "<span style=\"background-color: #FFFF00\">", "</span>", 1000));
                            results.add(result);
                            i++;
                        }
                        data.put("folders", results);
                    }
                }
            }


        } catch (Exception e) {
            LOG.error("Unable to retrieve data", e);
        }
        return renderer.render(template, data);
    }

    private String rewrite(String query) {
        String[] part = query.split("\\s+");
        int i = part.length-1;
        if (part.length > 0) {
            if (part[i].endsWith("*")) {
                part[i] = ("( title:" + part[i] + " OR body:" + part[i] + " )");
            } else {
                part[i] = ("( title:" + part[i] +" OR title:" + part[i]+ "* OR body:" + part[i] + " OR body:" + part[i] + "* )");
            }
        }
        for (i = 0; i < part.length-1; i++) {
            part[i] = "( title:" + part[i] + " OR body:" + part[i] + ")";
        }
        return Joiner.on(" AND ").join(part);
    }

    private MiruResponse<FullTextAnswer> query(MiruTenantId tenantId, MiruFilter filter, String query) throws Exception {

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
                    filter,
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
        return response;
    }

    @Override
    public String getTitle() {
        return "Wiki Query";
    }
}
