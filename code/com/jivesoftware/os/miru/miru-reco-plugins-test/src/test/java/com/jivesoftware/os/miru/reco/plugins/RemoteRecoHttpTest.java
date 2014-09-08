package com.jivesoftware.os.miru.reco.plugins;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.jivesoftware.os.jive.utils.http.client.HttpClientConfiguration;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactory;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactoryProvider;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFieldFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilterOperation;
import com.jivesoftware.os.miru.reco.plugins.reco.MiruRecoQueryCriteria;
import com.jivesoftware.os.miru.reco.plugins.reco.MiruRecoQueryParams;
import com.jivesoftware.os.miru.reco.plugins.reco.RecoConstants;
import com.jivesoftware.os.miru.reco.plugins.reco.RecoResult;
import com.jivesoftware.os.miru.reco.plugins.trending.MiruTrendingQueryCriteria;
import com.jivesoftware.os.miru.reco.plugins.trending.MiruTrendingQueryParams;
import com.jivesoftware.os.miru.reco.plugins.trending.TrendingConstants;
import com.jivesoftware.os.miru.reco.plugins.trending.TrendingResult;
import java.util.Arrays;
import java.util.Collections;
import org.apache.commons.io.Charsets;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;

/**
 *
 */
public class RemoteRecoHttpTest {

    private static final String REMOTE_HOST = "";
    private static final int REMOTE_PORT = -1;

    @Test(enabled = false, description = "Needs REMOTE constants")
    public void testSystemTrending() throws Exception {

        String tenant = "999";
        MiruTenantId tenantId = new MiruTenantId(tenant.getBytes(Charsets.UTF_8));

        HttpClientFactory httpClientFactory = new HttpClientFactoryProvider()
                .createHttpClientFactory(Collections.<HttpClientConfiguration>emptyList());
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new GuavaModule());
        RequestHelper requestHelper = new RequestHelper(httpClientFactory.createClient(REMOTE_HOST, REMOTE_PORT), objectMapper);

        MiruFilter constraitFilter = new MiruFilter(MiruFilterOperation.or,
                Optional.of(ImmutableList.of(
                        new MiruFieldFilter("objectType", Lists.transform(
                                Arrays.asList(102, 1, 18, 38, 801, 1464927464, -960826044),
                                Functions.toStringFunction())),
                        new MiruFieldFilter("activityType", Lists.transform(Arrays.asList(
                                0, //viewed
                                11, //liked
                                1, //created
                                65 //outcome_set
                        ), Functions.toStringFunction()))
                )),
                Optional.<ImmutableList<MiruFilter>>absent());

        MiruTrendingQueryCriteria.Builder queryCriteria = new MiruTrendingQueryCriteria.Builder()
                .setDesiredNumberOfDistincts(100)
                .setConstraintsFilter(constraitFilter)
                .setAggregateCountAroundField("parent")
                .setAuthzExpression(null);

        MiruTrendingQueryParams miruTrendingQueryParams = new MiruTrendingQueryParams(tenantId,
                queryCriteria.build());

        TrendingResult trendingResult = requestHelper.executeRequest(miruTrendingQueryParams,
                TrendingConstants.TRENDING_PREFIX + TrendingConstants.CUSTOM_QUERY_ENDPOINT,
                TrendingResult.class, TrendingResult.EMPTY_RESULTS);
        System.out.println(trendingResult);
        assertNotNull(trendingResult);
    }

    @Test(enabled = false, description = "Needs REMOTE constants")
    public void testSystemRecommended() throws Exception {

        String tenant = "999";
        MiruTenantId tenantId = new MiruTenantId(tenant.getBytes(Charsets.UTF_8));

        HttpClientFactory httpClientFactory = new HttpClientFactoryProvider()
                .createHttpClientFactory(Collections.<HttpClientConfiguration>emptyList());
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new GuavaModule());
        RequestHelper requestHelper = new RequestHelper(httpClientFactory.createClient(REMOTE_HOST, REMOTE_PORT), objectMapper);

        MiruFilter constraitFilter = new MiruFilter(MiruFilterOperation.and,
                Optional.of(ImmutableList.of(
                        new MiruFieldFilter("user", Arrays.asList(String.valueOf(3765))),
                        new MiruFieldFilter("objectType", Lists.transform(
                                Arrays.asList(102, 1, 18, 38, 801, 1464927464, -960826044),
                                Functions.toStringFunction())),
                        new MiruFieldFilter("activityType", Lists.transform(Arrays.asList(
                                0, //viewed
                                11, //liked
                                1, //created
                                65 //outcome_set
                        ),Functions.toStringFunction()))
                )),
                Optional.<ImmutableList<MiruFilter>>absent());

        MiruRecoQueryCriteria queryCriteria = new MiruRecoQueryCriteria(constraitFilter,
                new MiruAuthzExpression(Collections.<String>emptyList()),
                "parent", "parent", "parent",
                "user", "user", "user",
                "parent", "parent",
                100);

        MiruRecoQueryParams miruRecoQueryParams = new MiruRecoQueryParams(tenantId,
                queryCriteria);

        RecoResult results = requestHelper.executeRequest(miruRecoQueryParams,
                RecoConstants.RECO_PREFIX + RecoConstants.CUSTOM_QUERY_ENDPOINT,
                RecoResult.class, RecoResult.EMPTY_RESULTS);
        assertNotNull(results);
    }

}
