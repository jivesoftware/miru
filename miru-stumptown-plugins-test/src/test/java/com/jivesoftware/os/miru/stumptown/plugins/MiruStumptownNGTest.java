package com.jivesoftware.os.miru.stumptown.plugins;

import com.google.common.collect.ImmutableMap;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.miru.analytics.plugins.analytics.Analytics;
import com.jivesoftware.os.miru.analytics.plugins.analytics.AnalyticsAnswer;
import com.jivesoftware.os.miru.analytics.plugins.analytics.AnalyticsInjectable;
import com.jivesoftware.os.miru.analytics.plugins.analytics.AnalyticsQuery;
import com.jivesoftware.os.miru.analytics.plugins.analytics.AnalyticsQueryScoreSet;
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
import com.jivesoftware.os.miru.bitmaps.roaring6.MiruBitmapsRoaring;
import com.jivesoftware.os.miru.plugin.MiruProvider;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.plugin.test.MiruPluginTestBootstrap;
import com.jivesoftware.os.miru.service.MiruService;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * @author jonathan
 */
public class MiruStumptownNGTest {

    MiruSchema miruSchema = new MiruSchema.Builder("test", 1)
        .setFieldDefinitions(new MiruFieldDefinition[] {
            new MiruFieldDefinition(0, "user", MiruFieldDefinition.Type.singleTerm, MiruFieldDefinition.Prefix.NONE),
            new MiruFieldDefinition(1, "doc", MiruFieldDefinition.Type.singleTerm, MiruFieldDefinition.Prefix.NONE)
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
    ActivityUtil util = new ActivityUtil();

    MiruService service;
    AnalyticsInjectable injectable;

    @BeforeMethod
    public void setUpMethod() throws Exception {
        MiruProvider<MiruService> miruProvider = new MiruPluginTestBootstrap().bootstrap(tenant1, partitionId, miruHost,
            miruSchema, MiruBackingStorage.disk, new MiruBitmapsRoaring(), Collections.emptyList());

        this.service = miruProvider.getMiru(tenant1);

        this.injectable = new AnalyticsInjectable(miruProvider, new Analytics());
    }

    @Test(enabled = true)
    public void basicTest() throws Exception {

        Random rand = new Random(1_234);
        SnowflakeIdPacker snowflakeIdPacker = new SnowflakeIdPacker();
        int numberOfUsers = 10;
        int numberOfDocument = 1000;
        int numberOfViewsPerUser = 10;
        int numberOfActivities = numberOfUsers * numberOfViewsPerUser + 18;
        int numberOfBuckets = 32;
        long timespan = snowflakeIdPacker.pack(numberOfBuckets * TimeUnit.HOURS.toMillis(3), 0, 0);
        long intervalPerActivity = timespan / numberOfActivities;
        long smallestTime = snowflakeIdPacker.pack(System.currentTimeMillis(), 0, 0) - timespan;
        AtomicLong time = new AtomicLong(smallestTime);

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

        System.out.println("Running queries...");

        long lastTime = time.get();
        final MiruTimeRange timeRange = new MiruTimeRange(smallestTime, lastTime);
        for (int i = 0; i < numberOfUsers; i++) {
            String user = "bob" + i;
            MiruFieldFilter miruFieldFilter = MiruFieldFilter.ofTerms(MiruFieldType.primary, "user", user);
            MiruFilter filter = new MiruFilter(MiruFilterOperation.or, false, Collections.singletonList(miruFieldFilter), null);

            long s = System.currentTimeMillis();
            MiruRequest<AnalyticsQuery> request = new MiruRequest<>("test",
                tenant1,
                MiruActorId.NOT_PROVIDED,
                MiruAuthzExpression.NOT_PROVIDED,
                new AnalyticsQuery(
                    Collections.singletonList(new AnalyticsQueryScoreSet(
                        "test",
                        timeRange,
                        8)),
                    MiruFilter.NO_FILTER,
                    ImmutableMap.<String, MiruFilter>builder()
                        .put(user, filter)
                        .build()),
                MiruSolutionLogLevel.INFO);
            MiruResponse<AnalyticsAnswer> result = injectable.score(request);

            System.out.println("result:" + result);
            System.out.println("Took:" + (System.currentTimeMillis() - s));
        }

    }

}
