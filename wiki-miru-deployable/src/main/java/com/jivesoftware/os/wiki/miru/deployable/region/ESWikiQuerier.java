package com.jivesoftware.os.wiki.miru.deployable.region;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
        if (!input.userGuids.isEmpty()) {
            filter = filter(filter, "userGuid:(" + Joiner.on(" OR ").join(Splitter.on(",").omitEmptyStrings().trimResults().split(input.userGuids)) + ")");
        }

        if (!input.folderGuids.isEmpty()) {
            filter = filter(filter, "folderGuid:(" + Joiner.on(" OR ").join(Splitter.on(",").omitEmptyStrings().trimResults().split(input.folderGuids)) + ")");
        }

        SearchResponse response = client.prepareSearch("wiki")
            .setTypes("page")
            .setSearchType(SearchType.QUERY_AND_FETCH)
            .setFetchSource(new String[] { "userGuid", "folderGuid", "guid", "type" }, null)
            .setQuery(new QueryStringQueryBuilder(filter(filter, rewrite(input.query))))
            .setFrom(0).setSize(100).setExplain(false)
            .get();

        //"userGuid", "folderGuid", "guid", "type"
        int folderIndex = 0;
        int userIndex = 0;
        List<Result> results = Lists.newArrayList();
        for (SearchHit hit : response.getHits().getHits()) {


            results.add(new Result(
                hit.field("userGuid") != null ? hit.field("userGuid").getValue() : null,
                hit.field("folderGuid") != null ? hit.field("folderGuid").getValue() : null,
                hit.field("guid").getValue(),
                hit.field("type").getValue()
            ));


            if (hit.field("type").getValue().equals("content")) {
                contentKeys.add(hit.field("guid").getValue() + "-slug");
            } else {
                contentKeys.add(hit.field("guid").getValue());
            }
            if (hit.field("userGuid") != null) {

                String userGuid = hit.field("userGuid").getValue();
                if (userGuid != null && uniqueUsers.add(userGuid)) {
                    userKeys.add(userGuid);
                    usersIndex.put(userGuid, userIndex);
                    userIndex++;
                }
            }

            if (hit.field("folderGuid") != null) {
                String folderGuid = hit.field("folderGuid").getValue();
                if (folderGuid != null && uniqueFolders.add(folderGuid)) {
                    folderKeys.add(folderGuid);
                    foldersIndex.put(folderGuid, folderIndex);
                    folderIndex++;
                }
            }
        }


        return new Found(response.getTookInMillis(), response.getHits().getTotalHits(), results);
    }

    public String filter(String filter, String query) {
        return (query == null || query.isEmpty()) ? filter : "( " + filter + ") AND ( " + query + " )";
    }


    private String rewrite(String query) {
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


        SearchResponse response = client.prepareSearch("wiki")
            .setTypes("page")
            .setSearchType(SearchType.QUERY_AND_FETCH)
            .setFetchSource(new String[] { "userGuid", "folderGuid", "guid", "type" }, null)
            .setQuery(new QueryStringQueryBuilder(filter("type:user", rewrite(input.query))))
            .setFrom(0).setSize(100).setExplain(false)
            .get();

        List<Result> results = Lists.newArrayList();
        for (SearchHit hit : response.getHits().getHits()) {


            results.add(new Result(
                hit.field("userGuid") != null ? hit.field("userGuid").getValue() : null,
                hit.field("folderGuid") != null ? hit.field("folderGuid").getValue() : null,
                hit.field("guid").getValue(),
                hit.field("type").getValue()
            ));
        }

        return new Found(response.getTookInMillis(), response.getHits().getTotalHits(), results);
    }

    @Override
    public Found queryFolders(WikiMiruPluginRegionInput input) throws Exception {


        SearchResponse response = client.prepareSearch("wiki")
            .setTypes("page")
            .setSearchType(SearchType.QUERY_AND_FETCH)
            .setFetchSource(new String[] { "userGuid", "folderGuid", "guid", "type" }, null)
            .setQuery(new QueryStringQueryBuilder(filter("type:folder", rewrite(input.query))))
            .setFrom(0).setSize(100).setExplain(false)
            .get();

        List<Result> results = Lists.newArrayList();
        for (SearchHit hit : response.getHits().getHits()) {


            results.add(new Result(
                hit.field("userGuid") != null ? hit.field("userGuid").getValue() : null,
                hit.field("folderGuid") != null ? hit.field("folderGuid").getValue() : null,
                hit.field("guid").getValue(),
                hit.field("type").getValue()
            ));
        }

        return new Found(response.getTookInMillis(), response.getHits().getTotalHits(), results);
    }
}
