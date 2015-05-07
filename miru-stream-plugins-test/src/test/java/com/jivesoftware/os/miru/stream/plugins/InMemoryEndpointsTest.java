package com.jivesoftware.os.miru.stream.plugins;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.jivesoftware.os.jive.utils.id.Id;
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
import com.jivesoftware.os.miru.plugin.MiruProvider;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.plugin.test.MiruPluginTestBootstrap;
import com.jivesoftware.os.miru.service.MiruService;
import com.jivesoftware.os.miru.service.bitmap.MiruBitmapsRoaring;
import com.jivesoftware.os.miru.service.endpoint.MiruWriterEndpoints;
import com.jivesoftware.os.miru.stream.plugins.filter.AggregateCounts;
import com.jivesoftware.os.miru.stream.plugins.filter.AggregateCountsAnswer;
import com.jivesoftware.os.miru.stream.plugins.filter.AggregateCountsEndpoints;
import com.jivesoftware.os.miru.stream.plugins.filter.AggregateCountsInjectable;
import com.jivesoftware.os.miru.stream.plugins.filter.AggregateCountsQuery;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.ws.rs.core.Response;
import org.testng.annotations.BeforeMethod;
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

    @BeforeMethod
    public void setUp() throws Exception {

        MiruBackingStorage desiredStorage = MiruBackingStorage.memory;
        MiruHost miruHost = new MiruHost("logicalName", 1_234);
        MiruSchema schema = new MiruSchema.Builder("test", 1)
            .setFieldDefinitions(DefaultMiruSchemaDefinition.FIELDS)
            .build();

        MiruProvider<MiruService> miruProvider = new MiruPluginTestBootstrap().bootstrap(tenantId, partitionId, miruHost, schema, desiredStorage,
            new MiruBitmapsRoaring(), Collections.<MiruPartitionedActivity>emptyList());

        MiruService miruService = miruProvider.getMiru(tenantId);

        this.aggregateCountsEndpoints = new AggregateCountsEndpoints(
            new AggregateCountsInjectable(miruProvider, new AggregateCounts(miruProvider)));
        this.miruWriterEndpoints = new MiruWriterEndpoints(miruService, new MiruStats());
    }

    @Test(enabled = true, description = "Disabled until we can figure out a  better solution for bootstrapping instead of sleeping.")
    public void testSimpleAddActivities() throws Exception {
        AtomicLong time = new AtomicLong(0);
        AtomicInteger index = new AtomicInteger(0);

        List<MiruPartitionedActivity> partitionedActivities = Lists.newArrayList(
            activityFactory.activity(1, partitionId, index.incrementAndGet(),
                new MiruActivity.Builder(tenantId, time.incrementAndGet(), new String[]{}, 0)
                .putFieldValue(OBJECT_ID.getFieldName(), "value1")
                .putFieldValue(AUTHOR_ID.getFieldName(), "value2")
                .build()
            ),
            activityFactory.activity(1, partitionId, index.incrementAndGet(),
                new MiruActivity.Builder(tenantId, time.incrementAndGet(), new String[]{}, 0)
                .putFieldValue(OBJECT_ID.getFieldName(), "value1")
                .putFieldValue(AUTHOR_ID.getFieldName(), "value2")
                .build()
            ),
            activityFactory.activity(1, partitionId, index.incrementAndGet(),
                new MiruActivity.Builder(tenantId, time.incrementAndGet(), new String[]{}, 0)
                .putFieldValue(OBJECT_ID.getFieldName(), "value2")
                .putFieldValue(AUTHOR_ID.getFieldName(), "value3")
                .build()
            )
        );
        Response addResponse = miruWriterEndpoints.addActivities(partitionedActivities);
        assertNotNull(addResponse);
        assertEquals(addResponse.getStatus(), 200);

        JavaType type = objectMapper.getTypeFactory().constructParametricType(MiruResponse.class, AggregateCountsAnswer.class);

        // Request 1
        Response getResponse = aggregateCountsEndpoints.filterCustomStream(new MiruRequest<>(tenantId,
            new MiruActorId(Id.NULL),
            MiruAuthzExpression.NOT_PROVIDED,
            new AggregateCountsQuery(
                MiruStreamId.NULL,
                MiruTimeRange.ALL_TIME,
                MiruTimeRange.ALL_TIME,
                new MiruFilter(MiruFilterOperation.or,
                    false,
                    Arrays.asList(
                        new MiruFieldFilter(MiruFieldType.primary, OBJECT_ID.getFieldName(), ImmutableList.of("value2"))),
                    null),
                MiruFilter.NO_FILTER,
                OBJECT_ID.getFieldName(),
                0,
                100),
            MiruSolutionLogLevel.NONE));
        assertNotNull(getResponse);
        MiruResponse<AggregateCountsAnswer> result = objectMapper.readValue(getResponse.getEntity().toString(), type);
        assertEquals(result.answer.collectedDistincts, 1);

        // Request 2
        getResponse = aggregateCountsEndpoints.filterCustomStream(new MiruRequest<>(tenantId,
            new MiruActorId(Id.NULL),
            MiruAuthzExpression.NOT_PROVIDED,
            new AggregateCountsQuery(
                MiruStreamId.NULL,
                MiruTimeRange.ALL_TIME,
                MiruTimeRange.ALL_TIME,
                new MiruFilter(MiruFilterOperation.or,
                    false,
                    Arrays.asList(
                        new MiruFieldFilter(MiruFieldType.primary, OBJECT_ID.getFieldName(), ImmutableList.of("value2")),
                        new MiruFieldFilter(MiruFieldType.primary, AUTHOR_ID.getFieldName(), ImmutableList.of("value2"))),
                    null),
                MiruFilter.NO_FILTER,
                OBJECT_ID.getFieldName(),
                0,
                100),
            MiruSolutionLogLevel.NONE));
        assertNotNull(getResponse);
        result = objectMapper.readValue(getResponse.getEntity().toString(), type);
        assertEquals(result.answer.collectedDistincts, 2);
    }

}
