package com.jivesoftware.os.miru.service.endpoint.memory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.miru.api.MiruActorId;
import com.jivesoftware.os.miru.api.MiruAggregateCountsQueryCriteria;
import com.jivesoftware.os.miru.api.MiruAggregateCountsQueryParams;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruReader;
import com.jivesoftware.os.miru.api.MiruWriter;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivityFactory;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFieldFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilterOperation;
import com.jivesoftware.os.miru.api.query.result.AggregateCountsResult;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.service.MiruReaderWriterInMemoryModule;
import com.jivesoftware.os.miru.service.endpoint.MiruReaderEndpoints;
import com.jivesoftware.os.miru.service.endpoint.MiruWriterEndpoints;
import com.jivesoftware.os.miru.service.partition.MiruExpectedTenants;
import com.jivesoftware.os.miru.service.partition.MiruPartitionDirector;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.core.Response;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import static com.jivesoftware.os.miru.api.field.MiruFieldName.AUTHOR_ID;
import static com.jivesoftware.os.miru.api.field.MiruFieldName.OBJECT_ID;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

@Guice(modules = MiruReaderWriterInMemoryModule.class)
public class InMemoryMiruReaderWriterEndpointsTest {

    private MiruPartitionedActivityFactory activityFactory = new MiruPartitionedActivityFactory();
    private ObjectMapper objectMapper = new ObjectMapper();

    @Inject
    @Named("miruServiceHost")
    MiruHost miruHost;
    @Inject
    MiruReader miruReader;
    @Inject
    MiruWriter miruWriter;
    @Inject
    MiruClusterRegistry miruClusterRegistry;
    @Inject
    MiruExpectedTenants miruExpectedTenants;
    @Inject
    MiruPartitionDirector miruPartitionDirector;

    private MiruReaderEndpoints miruReaderEndpoints;
    private MiruWriterEndpoints miruWriterEndpoints;

    @BeforeMethod
    public void setUp() throws Exception {
        this.miruReaderEndpoints = new MiruReaderEndpoints(miruReader);
        this.miruWriterEndpoints = new MiruWriterEndpoints(miruWriter);
    }

    @Test(enabled = false, description = "Disabled until we can figure out a  better solution for bootstrapping instead of sleeping.")
    public void testSimpleAddActivities() throws Exception {
        AtomicLong time = new AtomicLong(0);
        AtomicInteger index = new AtomicInteger(0);

        MiruTenantId tenantId = new MiruTenantId("tenant1".getBytes(Charsets.UTF_8));
        miruExpectedTenants.expect(Lists.newArrayList(tenantId));

        miruReaderEndpoints.warm(tenantId);

        miruWriterEndpoints.addActivities(Lists.newArrayList(
            activityFactory.activity(1, MiruPartitionId.of(0), index.incrementAndGet(),
                new MiruActivity.Builder(tenantId, time.incrementAndGet(), new String[] { }, 0)
                    .putFieldValue(OBJECT_ID.getFieldName(), "value1")
                    .putFieldValue(AUTHOR_ID.getFieldName(), "value2")
                    .build())));

        List<MiruPartitionedActivity> partitionedActivities = Lists.newArrayList(
            activityFactory.activity(1, MiruPartitionId.of(0), index.incrementAndGet(),
                new MiruActivity.Builder(tenantId, time.incrementAndGet(), new String[] { }, 0)
                    .putFieldValue(OBJECT_ID.getFieldName(), "value1")
                    .putFieldValue(AUTHOR_ID.getFieldName(), "value2")
                    .build()
            ),
            activityFactory.activity(1, MiruPartitionId.of(0), index.incrementAndGet(),
                new MiruActivity.Builder(tenantId, time.incrementAndGet(), new String[] { }, 0)
                    .putFieldValue(OBJECT_ID.getFieldName(), "value2")
                    .putFieldValue(AUTHOR_ID.getFieldName(), "value3")
                    .build()
            )
        );
        Response addResponse = miruWriterEndpoints.addActivities(partitionedActivities);
        assertNotNull(addResponse);
        assertEquals(addResponse.getStatus(), 200);

        // Request 1
        Response getResponse = miruReaderEndpoints.filterCustomStream(new MiruAggregateCountsQueryParams(new MiruTenantId("tenant1".getBytes(Charsets.UTF_8)),
            Optional.of(new MiruActorId(new Id(1))),
            Optional.<MiruAuthzExpression>absent(),
            new MiruAggregateCountsQueryCriteria.Builder()
                .setAggregateCountAroundField(OBJECT_ID.getFieldName())
                .setStreamFilter(new MiruFilter(MiruFilterOperation.or,
                    Optional.of(ImmutableList.<MiruFieldFilter>of(
                        new MiruFieldFilter(OBJECT_ID.getFieldName(), ImmutableList.<MiruTermId>of(new MiruTermId("value2".getBytes(Charsets.UTF_8)))))),
                    Optional.<ImmutableList<MiruFilter>>absent()
                ))
                .build()
        ));
        assertNotNull(getResponse);
        AggregateCountsResult aggregateCountsResult = objectMapper.readValue(getResponse.getEntity().toString(), AggregateCountsResult.class);
        assertEquals(aggregateCountsResult.collectedDistincts, 1);

        // Request 2
        getResponse = miruReaderEndpoints.filterCustomStream(new MiruAggregateCountsQueryParams(new MiruTenantId("tenant1".getBytes(Charsets.UTF_8)),
            Optional.of(new MiruActorId(new Id(1))),
            Optional.<MiruAuthzExpression>absent(),
            new MiruAggregateCountsQueryCriteria.Builder()
                .setAggregateCountAroundField(OBJECT_ID.getFieldName())
                .setStreamFilter(new MiruFilter(MiruFilterOperation.or,
                    Optional.of(ImmutableList.<MiruFieldFilter>of(
                        new MiruFieldFilter(OBJECT_ID.getFieldName(), ImmutableList.<MiruTermId>of(new MiruTermId("value2".getBytes(Charsets.UTF_8)))),
                        new MiruFieldFilter(AUTHOR_ID.getFieldName(), ImmutableList.<MiruTermId>of(new MiruTermId("value2".getBytes(Charsets.UTF_8)))))),
                    Optional.<ImmutableList<MiruFilter>>absent()
                ))
                .build()
        ));
        assertNotNull(getResponse);
        aggregateCountsResult = objectMapper.readValue(getResponse.getEntity().toString(), AggregateCountsResult.class);
        assertEquals(aggregateCountsResult.collectedDistincts, 2);
    }

}
