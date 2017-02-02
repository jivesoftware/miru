package com.jivesoftware.os.wiki.miru.deployable.region;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.query.LuceneBackedQueryParser;
import com.jivesoftware.os.miru.ui.MiruPageRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.wiki.miru.deployable.WikiMiruIndexService.Content;
import com.jivesoftware.os.wiki.miru.deployable.region.WikiQuerier.Found;
import com.jivesoftware.os.wiki.miru.deployable.region.WikiQuerier.Result;
import com.jivesoftware.os.wiki.miru.deployable.storage.WikiMiruPayloadsAmza;
import java.util.ArrayList;
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
    private final WikiQuerierProvider wikiQuerierProvider;
    private final MiruSoyRenderer renderer;
    private final WikiMiruPayloadsAmza payloads;

    private final LuceneBackedQueryParser titleQueryParser = new LuceneBackedQueryParser("title");
    private final LuceneBackedQueryParser bodyQueryParser = new LuceneBackedQueryParser("body");

    public WikiQueryPluginRegion(String template,
        WikiQuerierProvider wikiQuerierProvider,
        MiruSoyRenderer renderer,
        WikiMiruPayloadsAmza payloads) {

        this.template = template;
        this.wikiQuerierProvider = wikiQuerierProvider;
        this.renderer = renderer;
        this.payloads = payloads;
    }

    @Override
    public String render(WikiMiruPluginRegionInput input) {
        return renderer.render(template, query(input));
    }

    public Map<String, Object> query(WikiMiruPluginRegionInput input) {
        Map<String, Object> data = Maps.newHashMap();
        try {
            int amzaGetCount = 0;
            long amzaGetElapsed = 0;
            data.put("querier", input.querier);
            data.put("tenantId", input.tenantId);
            data.put("query", input.query);

            WikiQuerier querier = wikiQuerierProvider.get(input.querier);


            if (!input.tenantId.trim().isEmpty()) {

                MiruTenantId tenantId = new MiruTenantId(input.tenantId.trim().getBytes(Charsets.UTF_8));

                List<String> contentKeys = Lists.newArrayList();
                List<String> folderKeys = Lists.newArrayList();
                List<String> userKeys = Lists.newArrayList();

                Set<String> uniqueFolders = new HashSet<>();
                Set<String> uniqueUsers = new HashSet<>();

                Map<String, Integer> foldersIndex = new HashMap<>();
                Map<String, Integer> usersIndex = new HashMap<>();

                Found found = querier.queryContent(input, uniqueFolders, uniqueUsers, foldersIndex, usersIndex, contentKeys,
                    folderKeys, userKeys);

                data.put("elapse", String.valueOf(found.elapse));
                data.put("count", String.valueOf(found.results.size()));
                data.put("found", String.valueOf(found.totalPossible));
                data.put("summary", "");


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
                amzaGetCount += allKeys.size();
                long elapsed = System.currentTimeMillis() - start;
                data.put("getContentElapse", String.valueOf(elapsed));
                amzaGetElapsed += elapsed;


                if (!found.results.isEmpty()) {
                    List<Map<String, Object>> results = new ArrayList<>();

                    start = System.currentTimeMillis();
                    int i = 0;
                    for (Result r : found.results) {
                        Content content = contents.get(i);
                        if (content != null) {

                            Map<String, Object> result = new HashMap<>();
                            result.put("type", r.type);

                            if (r.userGuid != null) {

                                Content userContent = contents.get(contentKeysCount + usersIndex.get(r.userGuid));
                                if (userContent != null) {
                                    result.put("userGuid", r.userGuid);
                                    result.put("user", userContent.title);
                                }
                            }

                            if (r.folderGuid != null) {
                                Content folderContent = contents.get(contentKeysCount + userKeysCount + foldersIndex.get(r.folderGuid));
                                if (folderContent != null) {
                                    result.put("folderGuid", r.folderGuid);
                                    result.put("folder", folderContent.title);
                                }
                            }

                            result.put("guid", r.guid);
                            result.put("title", content.title);
                            result.put("body", highlight("en", input.query, content.body));
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
                    result.put("body", highlight("en", input.query, content.body));

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
                    result.put("body", highlight("en", input.query, content.body));

                    data.put("queryFolders", result);
                }


                if (input.folderGuids.isEmpty() && input.userGuids.isEmpty()) {

                    Found users = querier.queryUsers(input);
                    data.put("usersElapse", String.valueOf(elapsed));
                    data.put("usersCount", String.valueOf(users.results.size()));
                    data.put("usersFound", String.valueOf((users.totalPossible)));

                    if (!users.results.isEmpty()) {


                        List<String> keys = Lists.newArrayList();
                        for (Result r : users.results) {
                            keys.add(r.guid);
                        }

                        long getStart = System.currentTimeMillis();
                        contents = payloads.multiGet(tenantId, keys, Content.class);
                        long getElapsed = System.currentTimeMillis() - getStart;
                        amzaGetCount += keys.size();
                        amzaGetElapsed += getElapsed;

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
                            result.put("body", highlight("en", input.query, content.body));
                            results.add(result);
                            i++;
                        }
                        data.put("users", results);
                    }

                    Found folders = querier.queryFolders(input);
                    data.put("foldersElapse", String.valueOf(elapsed));
                    data.put("foldersCount", String.valueOf(folders.results.size()));
                    data.put("foldersFound", String.valueOf((folders.totalPossible)));

                    if (!folders.results.isEmpty()) {

                        List<String> keys = Lists.newArrayList();
                        for (Result r : folders.results) {
                            keys.add(r.guid);
                        }

                        long getStart = System.currentTimeMillis();
                        contents = payloads.multiGet(tenantId, keys, Content.class);
                        long getElapsed = System.currentTimeMillis() - getStart;
                        amzaGetCount += keys.size();
                        amzaGetElapsed += getElapsed;

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
                            result.put("body", highlight("en", input.query, content.body));
                            results.add(result);
                            i++;
                        }
                        data.put("folders", results);
                    }
                }
            }

            data.put("amzaGetCount", String.valueOf(amzaGetCount));
            data.put("amzaGetElapse", String.valueOf(amzaGetElapsed));
            return data;

        } catch (Exception e) {
            LOG.error("Unable to retrieve data", e);
            return null;
        }
    }

    private String highlight(String locale, String query, String body) {
        return bodyQueryParser.highlight(locale, query, body, "<span style=\"background-color: #FFFF00\">", "</span>", 1000);
    }

    @Override
    public String getTitle() {
        return "Wiki Query";
    }
}
