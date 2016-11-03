package com.jivesoftware.os.miru.reco.plugins;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.miru.api.MiruActorId;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruValue;
import com.jivesoftware.os.miru.bitmaps.roaring5.MiruBitmapsRoaring;
import com.jivesoftware.os.miru.plugin.MiruProvider;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.plugin.test.MiruPluginTestBootstrap;
import com.jivesoftware.os.miru.reco.plugins.distincts.Distincts;
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsAnswer;
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsInjectable;
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsQuery;
import com.jivesoftware.os.miru.service.MiruService;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;

/**
 * @author jonathan
 */
public class MiruDistinctsNGTest {

    private MiruSchema miruSchema = new MiruSchema.Builder("test", 1)
            .setFieldDefinitions(new MiruFieldDefinition[]{
                    new MiruFieldDefinition(0, "user", MiruFieldDefinition.Type.singleTerm, MiruFieldDefinition.Prefix.NONE),
                    new MiruFieldDefinition(1, "doc", MiruFieldDefinition.Type.singleTerm, MiruFieldDefinition.Prefix.NONE),
                    new MiruFieldDefinition(2, "obj", MiruFieldDefinition.Type.multiTerm,
                            new MiruFieldDefinition.Prefix(MiruFieldDefinition.Prefix.Type.numeric, 4, ' ')),
                    new MiruFieldDefinition(3, "text", MiruFieldDefinition.Type.multiTerm, MiruFieldDefinition.Prefix.WILDCARD)
            })
            .build();

    private MiruTenantId tenant1 = new MiruTenantId("tenant1".getBytes());
    private MiruPartitionId partitionId = MiruPartitionId.of(1);

    private CollaborativeFilterUtil util = new CollaborativeFilterUtil();
    private AtomicInteger walIndex = new AtomicInteger();

    private int numberOfUsers = 2;
    private int numberOfViewsPerUser = 2;
    private int numberOfActivities = numberOfUsers * numberOfViewsPerUser + 18;
    private int numberOfBuckets = 32;

    private MiruService service;
    private DistinctsInjectable injectable;

    @BeforeMethod
    public void setUpMethod() throws Exception {
        MiruProvider<MiruService> miruProvider = new MiruPluginTestBootstrap().bootstrap(
                tenant1,
                partitionId,
                new MiruHost("logicalName"),
                miruSchema,
                MiruBackingStorage.disk,
                new MiruBitmapsRoaring(),
                Collections.emptyList());

        this.service = miruProvider.getMiru(tenant1);

        this.injectable = new DistinctsInjectable(miruProvider, new Distincts(miruProvider.getTermComposer()));
    }

    @Test(enabled = true)
    public void basicTest() throws Exception {
        SnowflakeIdPacker snowflakeIdPacker = new SnowflakeIdPacker();
        long timespan = numberOfBuckets * snowflakeIdPacker.pack(TimeUnit.HOURS.toMillis(3), 0, 0);
        long intervalPerActivity = timespan / numberOfActivities;
        AtomicLong time = new AtomicLong(snowflakeIdPacker.pack(System.currentTimeMillis(), 0, 0) - timespan);

        System.out.println("Building activities...");

        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "bob0", "1",
                walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "bob0", "2",
                walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "bob0", "2",
                walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "bob0", "3",
                walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "bob0", "3",
                walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "bob0", "4",
                walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "bob0", "4",
                walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "bob0", "5",
                walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "bob0", "9",
                walIndex.incrementAndGet())));

        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "frank", "1",
                walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "frank", "2",
                walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "frank", "2",
                walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "frank", "3",
                walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "frank", "3",
                walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "frank", "4",
                walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "frank", "4",
                walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "frank", "5",
                walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "frank", "10",
                walIndex.incrementAndGet())));

        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "jane", "2",
                walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "jane", "3",
                walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "jane", "3",
                walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "jane", "4",
                walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "jane", "4",
                walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "jane", "5",
                walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "jane", "11",
                walIndex.incrementAndGet())));

        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "liz", "3",
                walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "liz", "4",
                walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "liz", "4",
                walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "liz", "5",
                walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "liz", "12",
                walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "liz", "12",
                walIndex.incrementAndGet())));

        System.out.println("Running queries...");

        long lastTime = time.get();
        final MiruTimeRange timeRange = new MiruTimeRange(lastTime - timespan, lastTime);

        for (MiruFieldDefinition fieldDefinition : miruSchema.getFieldDefinitions()) {
            long s = System.currentTimeMillis();
            MiruRequest<DistinctsQuery> request = new MiruRequest<>("test",
                    tenant1,
                    MiruActorId.NOT_PROVIDED,
                    MiruAuthzExpression.NOT_PROVIDED,
                    new DistinctsQuery(timeRange,
                            fieldDefinition.name,
                            null,
                            MiruFilter.NO_FILTER,
                            null),
                    MiruSolutionLogLevel.INFO);
            MiruResponse<DistinctsAnswer> distinctsResult = injectable.gatherDistincts(request);

            System.out.println("distinctsResult: " + distinctsResult);
            System.out.println("took: " + (System.currentTimeMillis() - s));
        }
    }

    @Test
    public void rangeTest() throws Exception {
        SnowflakeIdPacker snowflakeIdPacker = new SnowflakeIdPacker();
        long timespan = numberOfBuckets * snowflakeIdPacker.pack(TimeUnit.HOURS.toMillis(3), 0, 0);
        long intervalPerActivity = timespan / numberOfActivities;
        AtomicLong time = new AtomicLong(snowflakeIdPacker.pack(System.currentTimeMillis(), 0, 0) - timespan);

        System.out.println("Building activities...");

        for (int i = 0; i < 100; i++) {
            service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "bob",
                    String.valueOf(i + 1), walIndex.incrementAndGet())));
        }

        System.out.println("Running queries...");

        long lastTime = time.get();
        final MiruTimeRange timeRange = new MiruTimeRange(lastTime - timespan, lastTime);
        MiruFieldDefinition fieldDefinition = miruSchema.getFieldDefinition(miruSchema.getFieldId("obj"));

        long s = System.currentTimeMillis();
        Set<String> types = Sets.newHashSet("0", "1", "2", "3", "8", "-1");
        MiruRequest<DistinctsQuery> request = new MiruRequest<>("test",
                tenant1,
                MiruActorId.NOT_PROVIDED,
                MiruAuthzExpression.NOT_PROVIDED,
                new DistinctsQuery(timeRange,
                        fieldDefinition.name,
                        null,
                        MiruFilter.NO_FILTER,
                        Lists.transform(Lists.newArrayList(types), MiruValue::new)),
                MiruSolutionLogLevel.INFO);
        MiruResponse<DistinctsAnswer> distinctsResult = injectable.gatherDistincts(request);
        for (MiruValue result : distinctsResult.answer.results) {
            String value = result.last();
            String type = value.substring(0, value.indexOf(' '));
            assertTrue(types.contains(type), "Unexpected type " + type);
        }

        System.out.println("distinctsResult: " + distinctsResult);
        System.out.println("count: " + distinctsResult.answer.results.size());
        System.out.println("took: " + (System.currentTimeMillis() - s));
    }

    @Test
    public void wildcardTest() throws Exception {
        SnowflakeIdPacker snowflakeIdPacker = new SnowflakeIdPacker();
        long timespan = numberOfBuckets * snowflakeIdPacker.pack(TimeUnit.HOURS.toMillis(3), 0, 0);
        long intervalPerActivity = timespan / numberOfActivities;
        AtomicLong time = new AtomicLong(snowflakeIdPacker.pack(System.currentTimeMillis(), 0, 0) - timespan);

        System.out.println("Building activities...");

        for (int i = 0; i < 100; i++) {
            service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "bob",
                    String.valueOf(i + 1), walIndex.incrementAndGet())));
        }

        System.out.println("Running queries...");

        long lastTime = time.get();
        final MiruTimeRange timeRange = new MiruTimeRange(lastTime - timespan, lastTime);
        MiruFieldDefinition fieldDefinition = miruSchema.getFieldDefinition(miruSchema.getFieldId("text"));

        long s = System.currentTimeMillis();
        Set<String> wildcards = Sets.newHashSet("ca", "do", "el", "mo");
        MiruRequest<DistinctsQuery> request = new MiruRequest<>(
                "test",
                tenant1,
                MiruActorId.NOT_PROVIDED,
                MiruAuthzExpression.NOT_PROVIDED,
                new DistinctsQuery(timeRange,
                        fieldDefinition.name,
                        null,
                        MiruFilter.NO_FILTER,
                        Lists.transform(Lists.newArrayList(wildcards), MiruValue::new)),
                MiruSolutionLogLevel.INFO);
        MiruResponse<DistinctsAnswer> distinctsResult = injectable.gatherDistincts(request);

        for (MiruValue result : distinctsResult.answer.results) {
            String wildcard = result.last().substring(0, 2);
            assertTrue(wildcards.contains(wildcard), "Unexpected wildcard " + wildcard);
        }

        System.out.println("distinctsResult: " + distinctsResult);
        System.out.println("count: " + distinctsResult.answer.results.size());
        System.out.println("took: " + (System.currentTimeMillis() - s));
    }

}
