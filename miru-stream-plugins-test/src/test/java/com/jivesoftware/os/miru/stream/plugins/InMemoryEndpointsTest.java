package com.jivesoftware.os.miru.stream.plugins;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.api.MiruActorId;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivityFactory;
import com.jivesoftware.os.miru.api.activity.schema.DefaultMiruSchemaDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFieldFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilterOperation;
import com.jivesoftware.os.miru.bitmaps.roaring6.MiruBitmapsRoaring;
import com.jivesoftware.os.miru.plugin.MiruProvider;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.plugin.test.MiruPluginTestBootstrap;
import com.jivesoftware.os.miru.service.MiruService;
import com.jivesoftware.os.miru.service.endpoint.MiruWriterEndpoints;
import com.jivesoftware.os.miru.stream.plugins.filter.AggregateCounts;
import com.jivesoftware.os.miru.stream.plugins.filter.AggregateCountsAnswer;
import com.jivesoftware.os.miru.stream.plugins.filter.AggregateCountsEndpoints;
import com.jivesoftware.os.miru.stream.plugins.filter.AggregateCountsInjectable;
import com.jivesoftware.os.miru.stream.plugins.filter.AggregateCountsQuery;
import com.jivesoftware.os.miru.stream.plugins.filter.AggregateCountsQueryConstraint;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.ws.rs.core.Response;
import org.testng.annotations.Test;

import static com.jivesoftware.os.miru.api.field.MiruFieldName.AUTHOR_ID;
import static com.jivesoftware.os.miru.api.field.MiruFieldName.OBJECT_ID;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class InMemoryEndpointsTest {

    MiruTenantId tenantId = new MiruTenantId("tenant1".getBytes());
    MiruPartitionId partitionId = MiruPartitionId.of(0);

    private final MiruPartitionedActivityFactory activityFactory = new MiruPartitionedActivityFactory();
    private final ObjectMapper objectMapper = new ObjectMapper();

    private AggregateCountsEndpoints aggregateCountsEndpoints;
    private MiruWriterEndpoints miruWriterEndpoints;

    @Test(enabled = true)
    public void testSimpleAddActivities() throws Exception {

        MiruBackingStorage desiredStorage = MiruBackingStorage.disk;
        MiruHost miruHost = new MiruHost("logicalName");
        MiruSchema schema = new MiruSchema.Builder("test", 1)
            .setFieldDefinitions(DefaultMiruSchemaDefinition.FIELDS)
            .build();
        AtomicLong time = new AtomicLong(0);
        AtomicInteger index = new AtomicInteger(0);
        List<MiruPartitionedActivity> partitionedActivities = Lists.newArrayList(
            activityFactory.activity(1, partitionId, index.incrementAndGet(),
                new MiruActivity.Builder(tenantId, time.incrementAndGet(), 0, false, new String[] {})
                    .putFieldValue(OBJECT_ID.getFieldName(), "value1")
                    .putFieldValue(AUTHOR_ID.getFieldName(), "value2")
                    .build()
            ),
            activityFactory.activity(1, partitionId, index.incrementAndGet(),
                new MiruActivity.Builder(tenantId, time.incrementAndGet(), 0, false, new String[] {})
                    .putFieldValue(OBJECT_ID.getFieldName(), "value1")
                    .putFieldValue(AUTHOR_ID.getFieldName(), "value2")
                    .build()
            ),
            activityFactory.activity(1, partitionId, index.incrementAndGet(),
                new MiruActivity.Builder(tenantId, time.incrementAndGet(), 0, false, new String[] {})
                    .putFieldValue(OBJECT_ID.getFieldName(), "value2")
                    .putFieldValue(AUTHOR_ID.getFieldName(), "value3")
                    .build()
            )
        );

        MiruProvider<MiruService> miruProvider = new MiruPluginTestBootstrap().bootstrap(tenantId, partitionId, miruHost, schema, desiredStorage,
            new MiruBitmapsRoaring(), partitionedActivities);

        MiruService miruService = miruProvider.getMiru(tenantId);

        this.aggregateCountsEndpoints = new AggregateCountsEndpoints(new AggregateCountsInjectable(miruProvider, new AggregateCounts(), null));
        this.miruWriterEndpoints = new MiruWriterEndpoints(miruService, new MiruStats());

        //Response addResponse = miruWriterEndpoints.addActivities(partitionedActivities);
        //assertNotNull(addResponse);
        //assertEquals(addResponse.getStatus(), 200);
        // Request 1
        Response getResponse = aggregateCountsEndpoints.filterCustomStream(new MiruRequest<>("test",
            tenantId,
            MiruActorId.NOT_PROVIDED,
            MiruAuthzExpression.NOT_PROVIDED,
            new AggregateCountsQuery(
                MiruStreamId.NULL,
                MiruFilter.NO_FILTER,
                MiruTimeRange.ALL_TIME,
                MiruTimeRange.ALL_TIME,
                MiruTimeRange.ALL_TIME,
                new MiruFilter(MiruFilterOperation.or,
                    false,
                    Collections.singletonList(MiruFieldFilter.ofTerms(MiruFieldType.primary, OBJECT_ID.getFieldName(), "value2")),
                    null),
                ImmutableMap.of("blah", new AggregateCountsQueryConstraint(
                    MiruFilter.NO_FILTER,
                    OBJECT_ID.getFieldName(),
                    0,
                    100,
                    new String[0])),
                false,
                false),
            MiruSolutionLogLevel.NONE));
        assertNotNull(getResponse);
        JavaType type = objectMapper.getTypeFactory().constructParametricType(MiruResponse.class, AggregateCountsAnswer.class);
        MiruResponse<AggregateCountsAnswer> result = objectMapper.readValue(getResponse.getEntity().toString(), type);
        assertEquals(result.answer.constraints.get("blah").collectedDistincts, 1);

        // Request 2
        getResponse = aggregateCountsEndpoints.filterCustomStream(new MiruRequest<>("test",
            tenantId,
            MiruActorId.NOT_PROVIDED,
            MiruAuthzExpression.NOT_PROVIDED,
            new AggregateCountsQuery(
                MiruStreamId.NULL,
                MiruFilter.NO_FILTER,
                MiruTimeRange.ALL_TIME,
                MiruTimeRange.ALL_TIME,
                MiruTimeRange.ALL_TIME,
                new MiruFilter(MiruFilterOperation.or,
                    false,
                    Arrays.asList(
                        MiruFieldFilter.ofTerms(MiruFieldType.primary, OBJECT_ID.getFieldName(), "value2"),
                        MiruFieldFilter.ofTerms(MiruFieldType.primary, AUTHOR_ID.getFieldName(), "value2")),
                    null),
                ImmutableMap.of("blah", new AggregateCountsQueryConstraint(
                    MiruFilter.NO_FILTER,
                    OBJECT_ID.getFieldName(),
                    0,
                    100,
                    new String[0])),
                false,
                false),
            MiruSolutionLogLevel.NONE));
        assertNotNull(getResponse);
        result = objectMapper.readValue(getResponse.getEntity().toString(), type);
        assertEquals(result.answer.constraints.get("blah").collectedDistincts, 2);
    }

}
