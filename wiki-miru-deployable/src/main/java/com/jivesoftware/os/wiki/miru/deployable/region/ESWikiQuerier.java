package com.jivesoftware.os.wiki.miru.deployable.region;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.search.SearchHit;

/**
 * Created by jonathan.colt on 12/1/16.
 */
public class ESWikiQuerier implements WikiQuerier {


    private final TransportClient client;

    public ESWikiQuerier(TransportClient client) {
        this.client = client;
    }


    @Override
    public Found queryContent(WikiMiruPluginRegionInput input,
        Set<String> uniqueFolders,
        Set<String> uniqueUsers,
        Map<String, Integer> foldersIndex,
        Map<String, Integer> usersIndex,
        List<String> contentKeys,
        List<String> folderKeys,
        List<String> userKeys) throws Exception {


        String filter = "type:content";
        filter = filter("tenant:" + input.tenantId, filter);

        if (!input.userGuids.isEmpty()) {
            filter = filter(filter, "userGuid:(" + Joiner.on(" OR ").join(Splitter.on(",").omitEmptyStrings().trimResults().split(input.userGuids)) + ")");
        }

        if (!input.folderGuids.isEmpty()) {
            filter = filter(filter, "folderGuid:(" + Joiner.on(" OR ").join(Splitter.on(",").omitEmptyStrings().trimResults().split(input.folderGuids)) + ")");
        }

        SearchResponse response = client.prepareSearch("wiki")
            .setTypes("page")
            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
            .setFetchSource(new String[] { "userGuid", "folderGuid", "guid", "type" }, null)
            .setQuery(new QueryStringQueryBuilder(filter(filter, rewrite(input.query))))
            .setFrom(0).setSize(100).setExplain(false)
            .get();

        //"userGuid", "folderGuid", "guid", "type"
        int folderIndex = 0;
        int userIndex = 0;
        List<Result> results = Lists.newArrayList();

        int count = 0;
        for (SearchHit hit : response.getHits().getHits()) {
            Map<String, Object> storedFields = hit.sourceAsMap();

            results.add(new Result(
                (String) storedFields.get("userGuid"),
                (String) storedFields.get("folderGuid"),
                (String) storedFields.get("guid"),
                (String) storedFields.get("type")
            ));


            if (storedFields.get("type").equals("content")) {
                contentKeys.add(storedFields.get("guid") + "-slug");
            } else {
                contentKeys.add((String) storedFields.get("guid"));
            }
            if (storedFields.get("userGuid") != null) {

                String userGuid = (String) storedFields.get("userGuid");
                if (userGuid != null && uniqueUsers.add(userGuid)) {
                    userKeys.add(userGuid);
                    usersIndex.put(userGuid, userIndex);
                    userIndex++;
                }
            }

            if (storedFields.get("folderGuid") != null) {
                String folderGuid = (String) storedFields.get("folderGuid");
                if (folderGuid != null && uniqueFolders.add(folderGuid)) {
                    folderKeys.add(folderGuid);
                    foldersIndex.put(folderGuid, folderIndex);
                    folderIndex++;
                }
            }
            count++;
            if (count == 100) {
                break;
            }
        }


        return new Found(response.getTookInMillis(), response.getHits().getTotalHits(), results);
    }

    public String filter(String filter, String query) {
        return (query == null || query.isEmpty()) ? filter : "( " + filter + ") AND ( " + query + " )";
    }


    private String rewrite(String query) {
        if (StringUtils.isBlank(query)) {
            return "";
        }

        String[] part = query.split("\\s+");
        int i = part.length - 1;
        if (part.length > 0) {
            if (part[i].endsWith("*")) {
                part[i] = ("( title:" + part[i] + " OR body:" + part[i] + " )");
            } else {
                part[i] = ("( title:" + part[i] + " OR title:" + part[i] + "* OR body:" + part[i] + " OR body:" + part[i] + "* )");
            }
        }
        for (i = 0; i < part.length - 1; i++) {
            part[i] = "( title:" + part[i] + " OR body:" + part[i] + ")";
        }
        return Joiner.on(" AND ").join(part);
    }


    @Override
    public Found queryUsers(WikiMiruPluginRegionInput input) throws Exception {

        String filter = "type:user";
        filter = filter("tenant:" + input.tenantId, filter);
        SearchResponse response = client.prepareSearch("wiki")
            .setTypes("page")
            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
            .setFetchSource(new String[] { "userGuid", "folderGuid", "guid", "type" }, null)
            .setQuery(new QueryStringQueryBuilder(filter(filter, rewrite(input.query))))
            .setFrom(0).setSize(100).setExplain(false)
            .get();

        List<Result> results = Lists.newArrayList();
        int count = 0;
        for (SearchHit hit : response.getHits().getHits()) {

            Map<String, Object> storedFields = hit.sourceAsMap();
            results.add(new Result(
                (String) storedFields.get("userGuid"),
                (String) storedFields.get("folderGuid"),
                (String) storedFields.get("guid"),
                (String) storedFields.get("type")
            ));
            count++;
            if (count == 100) {
                break;
            }
        }

        return new Found(response.getTookInMillis(), response.getHits().getTotalHits(), results);
    }

    @Override
    public Found queryFolders(WikiMiruPluginRegionInput input) throws Exception {

        String filter = "type:folder";
        filter = filter("tenant:" + input.tenantId, filter);
        SearchResponse response = client.prepareSearch("wiki")
            .setTypes("page")
            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
            .setFetchSource(new String[] { "userGuid", "folderGuid", "guid", "type" }, null)
            .setQuery(new QueryStringQueryBuilder(filter(filter, rewrite(input.query))))
            .setFrom(0).setSize(100).setExplain(false)
            .get();

        List<Result> results = Lists.newArrayList();
        int count = 0;
        for (SearchHit hit : response.getHits().getHits()) {


            Map<String, Object> storedFields = hit.sourceAsMap();
            results.add(new Result(
                (String) storedFields.get("userGuid"),
                (String) storedFields.get("folderGuid"),
                (String) storedFields.get("guid"),
                (String) storedFields.get("type")
            ));
            count++;
            if (count == 100) {
                break;
            }
        }

        return new Found(response.getTookInMillis(), response.getHits().getTotalHits(), results);
    }
}
