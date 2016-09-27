package com.jivesoftware.os.miru.stream.plugins;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.collect.ImmutableMap;
import com.jivesoftware.os.miru.api.MiruActorId;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFieldFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilterOperation;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.stream.plugins.filter.AggregateCountsAnswer;
import com.jivesoftware.os.miru.stream.plugins.filter.AggregateCountsConstants;
import com.jivesoftware.os.miru.stream.plugins.filter.AggregateCountsQuery;
import com.jivesoftware.os.miru.stream.plugins.filter.AggregateCountsQueryConstraint;
import com.jivesoftware.os.routing.bird.http.client.HttpClientConfiguration;
import com.jivesoftware.os.routing.bird.http.client.HttpClientFactory;
import com.jivesoftware.os.routing.bird.http.client.HttpClientFactoryProvider;
import com.jivesoftware.os.routing.bird.http.client.HttpRequestHelper;
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
        HttpRequestHelper requestHelper = new HttpRequestHelper(httpClientFactory.createClient(REMOTE_HOST, REMOTE_PORT), objectMapper);

        int queries = 100;
        for (int i = 0; i < queries; i++) {
            query(requestHelper, tenantId);
        }
    }

    private void query(HttpRequestHelper requestHelper, MiruTenantId tenantId) throws Exception {
        MiruRequest<AggregateCountsQuery> query = new MiruRequest<>("test",
            tenantId,
            MiruActorId.NOT_PROVIDED,
            MiruAuthzExpression.NOT_PROVIDED,
            new AggregateCountsQuery(
                MiruStreamId.NULL,
                MiruTimeRange.ALL_TIME,
                MiruTimeRange.ALL_TIME,
                MiruTimeRange.ALL_TIME,
                new MiruFilter(MiruFilterOperation.or,
                    false,
                    Collections.singletonList(MiruFieldFilter.ofTerms(MiruFieldType.primary, "activityType", 0)),
                    null),
                ImmutableMap.of("blah", new AggregateCountsQueryConstraint(MiruFilter.NO_FILTER, "parent", 0, 100, new String[0]))),
            MiruSolutionLogLevel.NONE);
        AggregateCountsAnswer result = requestHelper.executeRequest(query,
            AggregateCountsConstants.FILTER_PREFIX + AggregateCountsConstants.CUSTOM_QUERY_ENDPOINT,
            AggregateCountsAnswer.class, AggregateCountsAnswer.EMPTY_RESULTS);
        System.out.println(result);
        assertNotNull(result);
    }

}
