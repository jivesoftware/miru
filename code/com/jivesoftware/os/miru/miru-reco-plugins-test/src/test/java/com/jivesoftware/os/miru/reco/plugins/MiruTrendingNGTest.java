/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jivesoftware.os.miru.reco.plugins;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.miru.api.MiruActorId;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFieldFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilterOperation;
import com.jivesoftware.os.miru.plugin.MiruProvider;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.plugin.test.MiruPluginTestBootstrap;
import com.jivesoftware.os.miru.reco.plugins.trending.Trending;
import com.jivesoftware.os.miru.reco.plugins.trending.TrendingAnswer;
import com.jivesoftware.os.miru.reco.plugins.trending.TrendingInjectable;
import com.jivesoftware.os.miru.reco.plugins.trending.TrendingQuery;
import com.jivesoftware.os.miru.service.MiruService;
import com.jivesoftware.os.miru.service.bitmap.MiruBitmapsRoaring;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * @author jonathan
 */
public class MiruTrendingNGTest {

    MiruSchema miruSchema = new MiruSchema(
        new MiruFieldDefinition(0, "user", false, ImmutableList.of("doc"), ImmutableList.<String>of()),
        new MiruFieldDefinition(1, "doc", false, ImmutableList.of("user"), ImmutableList.of("user")));

    MiruTenantId tenant1 = new MiruTenantId("tenant1".getBytes());
    MiruPartitionId partitionId = MiruPartitionId.of(1);
    MiruHost miruHost = new MiruHost("logicalName", 1_234);
    CollaborativeFilterUtil util = new CollaborativeFilterUtil();

    MiruService service;
    TrendingInjectable injectable;

    @BeforeMethod
    public void setUpMethod() throws Exception {
        MiruProvider<MiruService> miruProvider = new MiruPluginTestBootstrap().bootstrap(tenant1, partitionId, miruHost,
            miruSchema, MiruBackingStorage.hybrid, new MiruBitmapsRoaring());

        this.service = miruProvider.getMiru(tenant1);
        this.injectable = new TrendingInjectable(miruProvider, new Trending());
    }

    @Test (enabled = true)
    public void basicTest() throws Exception {

        Random rand = new Random(1_234);
        SnowflakeIdPacker snowflakeIdPacker = new SnowflakeIdPacker();
        int numqueries = 2;
        int numberOfUsers = 2;
        int numberOfDocument = 100;
        int numberOfViewsPerUser = 2;
        int numberOfActivities = numberOfUsers * numberOfViewsPerUser + 18;
        int numberOfBuckets = 32;
        long timespan = numberOfBuckets * snowflakeIdPacker.pack(TimeUnit.HOURS.toMillis(3), 0, 0);
        long intervalPerActivity = timespan / numberOfActivities;
        AtomicLong time = new AtomicLong(snowflakeIdPacker.pack(System.currentTimeMillis(), 0, 0) - timespan);
        System.out.println("Building activities....");
        long start = System.currentTimeMillis();
        int count = 0;
        int numGroups = 10;
        for (int i = 0; i < numberOfUsers; i++) {
            String user = "bob" + i;
            int randSeed = i % numGroups;
            Random userRand = new Random(randSeed * 137);
            for (int r = 0; r < 2 * (i / numGroups); r++) {
                userRand.nextInt(numberOfDocument);
            }
            for (int d = 0; d < numberOfViewsPerUser; d++) {
                int docId = userRand.nextInt(numberOfDocument);
                long activityTime = time.addAndGet(intervalPerActivity);
                service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, activityTime, user, String.valueOf(docId))));
                if (++count % 10_000 == 0) {
                    System.out.println("Finished " + count + " in " + (System.currentTimeMillis() - start) + " ms");
                }
            }
        }

        System.out.println("Built and indexed " + count + " in " + (System.currentTimeMillis() - start) + "millis");

        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "bob0", "1")));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "bob0", "2")));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "bob0", "3")));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "bob0", "4")));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "bob0", "9")));

        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "frank", "1")));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "frank", "2")));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "frank", "3")));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "frank", "4")));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "frank", "10")));

        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "jane", "2")));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "jane", "3")));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "jane", "4")));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "jane", "11")));

        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "liz", "3")));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "liz", "4")));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "liz", "12")));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "liz", "12")));

        System.out.println("Running queries...");

        long lastTime = time.get();
        final MiruTimeRange timeRange = new MiruTimeRange(lastTime - timespan, lastTime);
        for (int i = 0; i < numqueries; i++) {
            String user = "bob" + rand.nextInt(numberOfUsers);
            MiruFieldFilter miruFieldFilter = new MiruFieldFilter("user", ImmutableList.of(user));
            MiruFilter filter = new MiruFilter(
                MiruFilterOperation.or,
                Optional.of(Arrays.asList(miruFieldFilter)),
                Optional.<List<MiruFilter>>absent());

            long s = System.currentTimeMillis();
            MiruRequest<TrendingQuery> request = new MiruRequest<>(tenant1,
                new MiruActorId(new Id(1)),
                MiruAuthzExpression.NOT_PROVIDED,
                new TrendingQuery(
                    timeRange,
                    32,
                    filter,
                    "doc",
                    10), true);
            MiruResponse<TrendingAnswer> trendingResult = injectable.scoreTrending(request);

            System.out.println("trendingResult:" + trendingResult);
            System.out.println("Took:" + (System.currentTimeMillis() - s));
        }

    }

}
