package com.jivesoftware.os.miru.siphon.deployable.region;

import com.google.common.collect.Lists;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SimpleQueryStringBuilder;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.elasticsearch.search.SearchHit;

/**
 * Created by jonathan.colt on 12/1/16.
 */
public class ESWikiQuerier implements WikiQuerier {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

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

        BoolQueryBuilder booleanQueryBuilder = new BoolQueryBuilder();
        booleanQueryBuilder.must(new SimpleQueryStringBuilder("content").field("type").analyzer("whitespace").lowercaseExpandedTerms(false));
        booleanQueryBuilder.must(new SimpleQueryStringBuilder(input.tenantId).field("tenant").analyzer("whitespace").lowercaseExpandedTerms(false));

        if (!input.userGuids.isEmpty()) {
            booleanQueryBuilder.must(new SimpleQueryStringBuilder(input.userGuids).field("userGuid").analyzer("whitespace").lowercaseExpandedTerms(false));
        }

        if (!input.folderGuids.isEmpty()) {
            booleanQueryBuilder.must(new SimpleQueryStringBuilder(input.folderGuids).field("folderGuid").analyzer("whitespace").lowercaseExpandedTerms(false));
        }

        rewrite(input.query, booleanQueryBuilder, input.wildcardExpansion);

        SearchResponse response = client.prepareSearch("wiki")
            .setTypes("page")
            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
            .setFetchSource(new String[] { "userGuid", "folderGuid", "guid", "type" }, null)
            .setQuery(booleanQueryBuilder)
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
        return (query == null || query.isEmpty()) ? filter : "+( " + filter + ") AND +( " + query + " )";
    }


    private QueryBuilder rewrite(String query, BoolQueryBuilder filter, boolean expandWildcards) {
        if (StringUtils.isBlank(query)) {
            return filter;
        }

        BoolQueryBuilder all = new BoolQueryBuilder();
        String[] part = query.split("\\s+");
        int i = part.length - 1;
        if (part.length > 0) {

            BoolQueryBuilder tail = new BoolQueryBuilder();
            if (part[i].endsWith("*")) {

                tail.should(new WildcardQueryBuilder("title", part[i]));
                tail.should(new WildcardQueryBuilder("body", part[i]));


            } else {

                tail.should(new SimpleQueryStringBuilder(part[i]).field("title").analyzer("english").locale(Locale.ENGLISH).lowercaseExpandedTerms(
                    true).analyzeWildcard(false));
                tail.should(
                    new SimpleQueryStringBuilder(part[i]).field("body").analyzer("english").locale(Locale.ENGLISH).lowercaseExpandedTerms(true).analyzeWildcard(
                        false));

                if (expandWildcards) {
                    tail.should(new WildcardQueryBuilder("title", part[i] + "*"));
                    tail.should(new WildcardQueryBuilder("body", part[i] + "*"));
                }

            }
            all.must(tail);
        }

        for (i = 0; i < part.length - 1; i++) {
            BoolQueryBuilder b = new BoolQueryBuilder();
            b.should(
                new SimpleQueryStringBuilder(part[i]).field("title").analyzer("english").locale(Locale.ENGLISH).lowercaseExpandedTerms(true).analyzeWildcard(
                    false));
            b.should(
                new SimpleQueryStringBuilder(part[i]).field("body").analyzer("english").locale(Locale.ENGLISH).lowercaseExpandedTerms(true).analyzeWildcard(
                    false));
            all.must(b);
        }

        filter.must(all);
        return filter;
    }


    @Override
    public Found queryUsers(WikiMiruPluginRegionInput input) throws Exception {

        BoolQueryBuilder booleanQueryBuilder = new BoolQueryBuilder();
        booleanQueryBuilder.must(new SimpleQueryStringBuilder("user").field("type").analyzer("whitespace").lowercaseExpandedTerms(false));
        booleanQueryBuilder.must(new SimpleQueryStringBuilder(input.tenantId).field("tenant").analyzer("whitespace").lowercaseExpandedTerms(false));
        rewrite(input.query, booleanQueryBuilder, input.wildcardExpansion);

        SearchResponse response = client.prepareSearch("wiki")
            .setTypes("page")
            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
            .setFetchSource(new String[] { "userGuid", "folderGuid", "guid", "type" }, null)
            .setQuery(booleanQueryBuilder)
            .setFrom(0)
            .setSize(100)
            .setExplain(false)
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

        BoolQueryBuilder booleanQueryBuilder = new BoolQueryBuilder();
        booleanQueryBuilder.must(new SimpleQueryStringBuilder("folder").field("type").analyzer("whitespace").lowercaseExpandedTerms(false));
        booleanQueryBuilder.must(new SimpleQueryStringBuilder(input.tenantId).field("tenant").analyzer("whitespace").lowercaseExpandedTerms(false));
        rewrite(input.query, booleanQueryBuilder, input.wildcardExpansion);

        SearchResponse response = client.prepareSearch("wiki")
            .setTypes("page")
            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
            .setFetchSource(new String[] { "userGuid", "folderGuid", "guid", "type" }, null)
            .setQuery(booleanQueryBuilder)
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
