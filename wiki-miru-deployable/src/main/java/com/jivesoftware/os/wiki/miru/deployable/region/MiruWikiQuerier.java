package com.jivesoftware.os.wiki.miru.deployable.region;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.api.MiruActorId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFieldFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilterOperation;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.stream.plugins.fulltext.FullTextAnswer;
import com.jivesoftware.os.miru.stream.plugins.fulltext.FullTextAnswer.ActivityScore;
import com.jivesoftware.os.miru.stream.plugins.fulltext.FullTextConstants;
import com.jivesoftware.os.miru.stream.plugins.fulltext.FullTextQuery;
import com.jivesoftware.os.miru.stream.plugins.fulltext.FullTextQuery.Strategy;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.http.client.HttpResponse;
import com.jivesoftware.os.routing.bird.http.client.HttpResponseMapper;
import com.jivesoftware.os.routing.bird.http.client.RoundRobinStrategy;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.routing.bird.shared.ClientCall.ClientResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by jonathan.colt on 12/1/16.
 */
public class MiruWikiQuerier implements WikiQuerier {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final TenantAwareHttpClient<String> readerClient;
    private final ObjectMapper requestMapper;
    private final HttpResponseMapper responseMapper;

    public MiruWikiQuerier(TenantAwareHttpClient<String> readerClient, ObjectMapper requestMapper, HttpResponseMapper responseMapper) {
        this.readerClient = readerClient;
        this.requestMapper = requestMapper;
        this.responseMapper = responseMapper;
    }


    @Override
    public Found queryFolders(WikiMiruPluginRegionInput input) throws Exception {

        MiruTenantId tenantId = new MiruTenantId(input.tenantId.getBytes(StandardCharsets.UTF_8));
        String query = input.query.isEmpty() ? "" : rewrite(input.query.toLowerCase());

        MiruFilter foldersFilter = new MiruFilter(MiruFilterOperation.and, false, Arrays.asList(MiruFieldFilter.of(MiruFieldType.primary, "type",
            Arrays.asList("folder"))), null);

        long start = System.currentTimeMillis();
        MiruResponse<FullTextAnswer> folders = query(tenantId, foldersFilter, query);
        long elapsed = System.currentTimeMillis() - start;

        List<Result> results = Lists.newArrayList();
        if (folders != null && folders.answer != null) {
            for (ActivityScore score : folders.answer.results) {
                results.add(new Result(
                    score.values[0] == null ? null : ((score.values[0].length == 0) ? null : score.values[0][0].last()),
                    score.values[1] == null ? null : ((score.values[1].length == 0) ? null : score.values[1][0].last()),
                    score.values[2] == null ? null : ((score.values[2].length == 0) ? null : score.values[2][0].last()),
                    score.values[3] == null ? null : ((score.values[3].length == 0) ? null : score.values[3][0].last())
                ));
            }
            return new Found(elapsed, folders.answer.found, results);
        } else {
            return new Found(0, 0, Collections.emptyList());
        }
    }

    @Override
    public Found queryUsers(WikiMiruPluginRegionInput input) throws Exception {
        MiruTenantId tenantId = new MiruTenantId(input.tenantId.getBytes(StandardCharsets.UTF_8));

        String query = input.query.isEmpty() ? "" : rewrite(input.query.toLowerCase());

        MiruFilter usersFilter = new MiruFilter(MiruFilterOperation.and, false, Arrays.asList(MiruFieldFilter.of(MiruFieldType.primary, "type",
            Arrays.asList("user"))), null);

        long start = System.currentTimeMillis();
        MiruResponse<FullTextAnswer> users = query(tenantId, usersFilter, query);
        long elapsed = System.currentTimeMillis() - start;
        List<Result> results = Lists.newArrayList();
        if (users != null && users.answer != null) {
            for (ActivityScore score : users.answer.results) {
                results.add(new Result(
                    score.values[0] == null ? null : ((score.values[0].length == 0) ? null : score.values[0][0].last()),
                    score.values[1] == null ? null : ((score.values[1].length == 0) ? null : score.values[1][0].last()),
                    score.values[2] == null ? null : ((score.values[2].length == 0) ? null : score.values[2][0].last()),
                    score.values[3] == null ? null : ((score.values[3].length == 0) ? null : score.values[3][0].last())
                ));
            }
            return new Found(elapsed, users.answer.found, results);
        } else {
            return new Found(0, 0, Collections.emptyList());
        }

    }

    public Found queryContent(WikiMiruPluginRegionInput input,
        Set<String> uniqueFolders,
        Set<String> uniqueUsers,
        Map<String, Integer> foldersIndex,
        Map<String, Integer> usersIndex,
        List<String> contentKeys,
        List<String> folderKeys,
        List<String> userKeys) throws Exception {

        MiruTenantId tenantId = new MiruTenantId(input.tenantId.getBytes(StandardCharsets.UTF_8));
        String query = input.query.isEmpty() ? "" : rewrite(input.query.toLowerCase());


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

        List<Result> results = Lists.newArrayList();
        if (response != null && response.answer != null) {
            LOG.info("Found:{} for {}:{}", response.answer.results.size(), input.tenantId, query);

            List<ActivityScore> scores = response.answer.results;


            int folderIndex = 0;
            int userIndex = 0;

            for (ActivityScore score : scores) {
                results.add(new Result(
                    score.values[0] == null ? null : ((score.values[0].length == 0) ? null : score.values[0][0].last()),
                    score.values[1] == null ? null : ((score.values[1].length == 0) ? null : score.values[1][0].last()),
                    score.values[2] == null ? null : ((score.values[2].length == 0) ? null : score.values[2][0].last()),
                    score.values[3] == null ? null : ((score.values[3].length == 0) ? null : score.values[3][0].last())
                ));


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

            //ObjectMapper mapper = new ObjectMapper();
            //mapper.enable(SerializationFeature.INDENT_OUTPUT);
            //data.put("summary", Joiner.on("\n").join(response.log) + "\n\n" + mapper.writeValueAsString(response.solutions));
            return new Found(response.totalElapsed, response.answer.found, results);
        } else {
            LOG.warn("Empty full text response from {}", tenantId);
            return new Found(0, 0, Collections.emptyList());
        }


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
}
