package com.jivesoftware.os.miru.stream.plugins;

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
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFieldFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilterOperation;
import com.jivesoftware.os.miru.query.MiruTimeRange;
import com.jivesoftware.os.miru.stream.plugins.filter.AggregateCountsConstants;
import com.jivesoftware.os.miru.stream.plugins.filter.AggregateCountsQuery;
import com.jivesoftware.os.miru.stream.plugins.filter.AggregateCountsResult;
import java.util.Arrays;
import java.util.Collections;
import org.apache.commons.io.Charsets;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;

/**
 *
 */
public class RemoteStreamHttpTest {

    private static final String REMOTE_HOST = "";
    private static final int REMOTE_PORT = -1;

    @Test(enabled = false, description = "Needs REMOTE constants")
    public void testAggregateCounts() throws Exception {

        String tenant = "999";
        MiruTenantId tenantId = new MiruTenantId(tenant.getBytes(Charsets.UTF_8));

        HttpClientFactory httpClientFactory = new HttpClientFactoryProvider()
                .createHttpClientFactory(Collections.<HttpClientConfiguration>emptyList());
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new GuavaModule());
        RequestHelper requestHelper = new RequestHelper(httpClientFactory.createClient(REMOTE_HOST, REMOTE_PORT), objectMapper);

        int queries = 100;
        for (int i = 0; i < queries; i++) {
            query(requestHelper, tenantId);
        }
    }

    private void query(RequestHelper requestHelper, MiruTenantId tenantId) throws Exception {
        AggregateCountsQuery query = new AggregateCountsQuery(tenantId,
                MiruStreamId.NULL,
                MiruTimeRange.ALL_TIME,
                MiruTimeRange.ALL_TIME,
                new MiruFilter(MiruFilterOperation.or,
                        Optional.of(ImmutableList.of(
                                new MiruFieldFilter("activityType", Lists.transform(Arrays.asList(
                                        0 //viewed
                                ), Functions.toStringFunction()))
                        )),
                        Optional.<ImmutableList<MiruFilter>>absent()),
                MiruFilter.NO_FILTER,
                MiruAuthzExpression.NOT_PROVIDED,
                "parent",
                0,
                100);
        AggregateCountsResult result = requestHelper.executeRequest(query,
                AggregateCountsConstants.FILTER_PREFIX + AggregateCountsConstants.CUSTOM_QUERY_ENDPOINT,
                AggregateCountsResult.class, AggregateCountsResult.EMPTY_RESULTS);
        System.out.println(result);
        assertNotNull(result);
    }

}
