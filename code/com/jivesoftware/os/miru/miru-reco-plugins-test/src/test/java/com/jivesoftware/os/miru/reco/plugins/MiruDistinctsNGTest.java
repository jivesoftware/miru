/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jivesoftware.os.miru.reco.plugins;

import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.miru.api.MiruActorId;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
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
import com.jivesoftware.os.miru.service.bitmap.MiruBitmapsRoaring;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * @author jonathan
 */
public class MiruDistinctsNGTest {

    MiruSchema miruSchema = new MiruSchema(
        new MiruFieldDefinition(0, "user"),
        new MiruFieldDefinition(1, "doc"));

    MiruTenantId tenant1 = new MiruTenantId("tenant1".getBytes());
    MiruPartitionId partitionId = MiruPartitionId.of(1);
    MiruHost miruHost = new MiruHost("logicalName", 1_234);
    CollaborativeFilterUtil util = new CollaborativeFilterUtil();
    AtomicInteger walIndex = new AtomicInteger();

    int numqueries = 2;
    int numberOfUsers = 2;
    int numberOfDocument = 100;
    int numberOfViewsPerUser = 2;
    int numberOfActivities = numberOfUsers * numberOfViewsPerUser + 18;
    int numberOfBuckets = 32;

    MiruService service;
    DistinctsInjectable injectable;

    @BeforeMethod
    public void setUpMethod() throws Exception {
        MiruProvider<MiruService> miruProvider = new MiruPluginTestBootstrap().bootstrap(tenant1, partitionId, miruHost,
            miruSchema, MiruBackingStorage.memory, new MiruBitmapsRoaring(), Collections.<MiruPartitionedActivity>emptyList());

        this.service = miruProvider.getMiru(tenant1);
        this.injectable = new DistinctsInjectable(miruProvider, new Distincts());
    }

    @Test(enabled = true)
    public void basicTest() throws Exception {

        Random rand = new Random(1_234);
        SnowflakeIdPacker snowflakeIdPacker = new SnowflakeIdPacker();
        long timespan = numberOfBuckets * snowflakeIdPacker.pack(TimeUnit.HOURS.toMillis(3), 0, 0);
        long intervalPerActivity = timespan / numberOfActivities;
        AtomicLong time = new AtomicLong(snowflakeIdPacker.pack(System.currentTimeMillis(), 0, 0) - timespan);
        System.out.println("Building activities....");

        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "bob0",
            Arrays.asList("1", "2"),
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "bob0",
            Arrays.asList("2", "3"),
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "bob0",
            Arrays.asList("3", "4"),
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "bob0",
            Arrays.asList("4", "5"),
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "bob0",
            Arrays.asList("9"),
            walIndex.incrementAndGet())));

        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "frank",
            Arrays.asList("1", "2"),
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "frank",
            Arrays.asList("2", "3"),
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "frank",
            Arrays.asList("3", "4"),
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "frank",
            Arrays.asList("4", "5"),
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "frank",
            Arrays.asList("10"),
            walIndex.incrementAndGet())));

        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "jane",
            Arrays.asList("2", "3"),
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "jane",
            Arrays.asList("3", "4"),
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "jane",
            Arrays.asList("4", "5"),
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "jane",
            Arrays.asList("11"),
            walIndex.incrementAndGet())));

        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "liz",
            Arrays.asList("3", "4"),
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "liz",
            Arrays.asList("4", "5"),
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "liz",
            Arrays.asList("12"),
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "liz",
            Arrays.asList("12"),
            walIndex.incrementAndGet())));

        System.out.println("Running queries...");

        long lastTime = time.get();
        final MiruTimeRange timeRange = new MiruTimeRange(lastTime - timespan, lastTime);

        for (MiruFieldDefinition fieldDefinition : miruSchema.getFieldDefinitions()) {
            long s = System.currentTimeMillis();
            MiruRequest<DistinctsQuery> request = new MiruRequest<>(tenant1,
                new MiruActorId(new Id(1)),
                MiruAuthzExpression.NOT_PROVIDED,
                new DistinctsQuery(timeRange, MiruFilter.NO_FILTER, fieldDefinition.name),
                MiruSolutionLogLevel.INFO);
            MiruResponse<DistinctsAnswer> distinctsResult = injectable.gatherDistincts(request);

            System.out.println("distinctsResult:" + distinctsResult);
            System.out.println("Took:" + (System.currentTimeMillis() - s));
        }

    }

}
