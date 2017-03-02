/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jivesoftware.os.miru.reco.plugins;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.miru.analytics.plugins.analytics.Analytics;
import com.jivesoftware.os.miru.api.MiruActorId;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFieldFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilterOperation;
import com.jivesoftware.os.miru.api.query.filter.MiruValue;
import com.jivesoftware.os.miru.bitmaps.roaring6.MiruBitmapsRoaring;
import com.jivesoftware.os.miru.plugin.MiruProvider;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.plugin.test.MiruPluginTestBootstrap;
import com.jivesoftware.os.miru.reco.plugins.distincts.Distincts;
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsQuery;
import com.jivesoftware.os.miru.reco.plugins.trending.TrendingAnswer;
import com.jivesoftware.os.miru.reco.plugins.trending.TrendingInjectable;
import com.jivesoftware.os.miru.reco.plugins.trending.TrendingQuery;
import com.jivesoftware.os.miru.reco.plugins.trending.TrendingQueryScoreSet;
import com.jivesoftware.os.miru.service.MiruService;
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
public class MiruTrendingNGTest {

    MiruSchema miruSchema = new MiruSchema.Builder("test", 1)
        .setFieldDefinitions(new MiruFieldDefinition[] {
            new MiruFieldDefinition(0, "user", MiruFieldDefinition.Type.singleTerm, MiruFieldDefinition.Prefix.NONE),
            new MiruFieldDefinition(1, "doc", MiruFieldDefinition.Type.singleTerm, MiruFieldDefinition.Prefix.NONE),
            new MiruFieldDefinition(2, "obj", MiruFieldDefinition.Type.singleTerm,
                new MiruFieldDefinition.Prefix(MiruFieldDefinition.Prefix.Type.numeric, 4, ' '))
        })
        .setPairedLatest(ImmutableMap.of(
            "user", Arrays.asList("doc"),
            "doc", Arrays.asList("user")))
        .setBloom(ImmutableMap.of(
            "doc", Arrays.asList("user")))
        .build();

    MiruTenantId tenant1 = new MiruTenantId("tenant1".getBytes());
    MiruPartitionId partitionId = MiruPartitionId.of(1);
    MiruHost miruHost = new MiruHost("logicalName");
    CollaborativeFilterUtil util = new CollaborativeFilterUtil();
    AtomicInteger walIndex = new AtomicInteger();

    int numqueries = 2;
    int numberOfUsers = 2;
    int numberOfDocument = 100;
    int numberOfViewsPerUser = 2;
    int numberOfActivities = numberOfUsers * numberOfViewsPerUser + 18;
    int numberOfBuckets = 32;

    MiruService service;
    TrendingInjectable injectable;

    @BeforeMethod
    public void setUpMethod() throws Exception {
        MiruProvider<MiruService> miruProvider = new MiruPluginTestBootstrap().bootstrap(tenant1, partitionId, miruHost,
            miruSchema, MiruBackingStorage.disk, new MiruBitmapsRoaring(), Collections.emptyList());

        this.service = miruProvider.getMiru(tenant1);

        this.injectable = new TrendingInjectable(miruProvider, new Distincts(miruProvider.getTermComposer()), new Analytics());
    }

    @Test(enabled = true)
    public void basicTest() throws Exception {

        Random rand = new Random(1_234);
        SnowflakeIdPacker snowflakeIdPacker = new SnowflakeIdPacker();
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
                service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, activityTime, user, String.valueOf(docId),
                    walIndex.incrementAndGet())));
                if (++count % 10_000 == 0) {
                    System.out.println("Finished " + count + " in " + (System.currentTimeMillis() - start) + " ms");
                }
            }
        }

        System.out.println("Built and indexed " + count + " in " + (System.currentTimeMillis() - start) + "millis");

        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "bob0", "1",
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "bob0", "2",
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "bob0", "3",
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "bob0", "4",
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "bob0", "9",
            walIndex.incrementAndGet())));

        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "frank", "1",
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "frank", "2",
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "frank", "3",
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "frank", "4",
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "frank", "10",
            walIndex.incrementAndGet())));

        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "jane", "2",
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "jane", "3",
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "jane", "4",
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "jane", "11",
            walIndex.incrementAndGet())));

        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "liz", "3",
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "liz", "4",
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "liz", "12",
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.addAndGet(intervalPerActivity), "liz", "12",
            walIndex.incrementAndGet())));

        System.out.println("Running queries...");

        long lastTime = time.get();
        final MiruTimeRange timeRange = new MiruTimeRange(lastTime - timespan, lastTime);
        for (int i = 0; i < numqueries; i++) {
            String user = "bob" + rand.nextInt(numberOfUsers);
            MiruFieldFilter miruFieldFilter = MiruFieldFilter.ofTerms(MiruFieldType.primary, "user", user);
            MiruFilter filter = new MiruFilter(MiruFilterOperation.or, false, Collections.singletonList(miruFieldFilter), null);

            long s = System.currentTimeMillis();
            MiruRequest<TrendingQuery> request = new MiruRequest<>("test",
                tenant1,
                MiruActorId.NOT_PROVIDED,
                MiruAuthzExpression.NOT_PROVIDED,
                new TrendingQuery(
                    Collections.singletonList(new TrendingQueryScoreSet(
                        "test",
                        Collections.singleton(TrendingQuery.Strategy.LINEAR_REGRESSION),
                        timeRange,
                        32,
                        10)),
                    filter,
                    "obj",
                    Collections.singletonList(Collections.singletonList(new DistinctsQuery(
                        timeRange,
                        "obj",
                        null,
                        MiruFilter.NO_FILTER,
                        Lists.transform(Arrays.asList("0", "2", "8", "-1"), MiruValue::new))))),
                MiruSolutionLogLevel.INFO);
            MiruResponse<TrendingAnswer> trendingResult = injectable.scoreTrending(request);

            System.out.println("trendingResult:" + trendingResult);
            System.out.println("Took:" + (System.currentTimeMillis() - s));
        }

    }

}
