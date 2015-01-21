package com.jivesoftware.os.miru.reco.plugins;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.miru.api.MiruActorId;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivityFactory;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFieldFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilterOperation;
import com.jivesoftware.os.miru.plugin.MiruProvider;
import com.jivesoftware.os.miru.plugin.index.MiruIndexUtil;
import com.jivesoftware.os.miru.plugin.solution.MiruAggregateUtil;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.plugin.test.MiruPluginTestBootstrap;
import com.jivesoftware.os.miru.reco.plugins.reco.CollaborativeFiltering;
import com.jivesoftware.os.miru.reco.plugins.reco.RecoAnswer;
import com.jivesoftware.os.miru.reco.plugins.reco.RecoInjectable;
import com.jivesoftware.os.miru.reco.plugins.reco.RecoQuery;
import com.jivesoftware.os.miru.service.MiruService;
import com.jivesoftware.os.miru.service.bitmap.MiruBitmapsRoaring;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * @author jonathan
 */
public class MiruCollaborativeFilterNGTest {

    MiruSchema miruSchema = new MiruSchema(
        new MiruFieldDefinition(0, "user", false, ImmutableList.of("doc"), ImmutableList.<String>of()),
        new MiruFieldDefinition(1, "doc", false, ImmutableList.of("user"), ImmutableList.of("user")));

    MiruTenantId tenant1 = new MiruTenantId("tenant1".getBytes());
    MiruPartitionId partitionId = MiruPartitionId.of(1);
    MiruHost miruHost = new MiruHost("logicalName", 1_234);
    CollaborativeFilterUtil util = new CollaborativeFilterUtil();
    MiruIndexUtil indexUtil = new MiruIndexUtil();
    AtomicInteger time = new AtomicInteger();
    AtomicInteger walIndex = new AtomicInteger();
    Random rand = new Random(1_234);

    int numqueries = 2;
    int numberOfUsers = 2;
    int numberOfDocument = 1000;
    int numberOfViewsPerUser = 100;

    MiruService service;
    RecoInjectable injectable;

    @BeforeMethod
    public void setUpMethod() throws Exception {
        MiruPartitionedActivityFactory factory = new MiruPartitionedActivityFactory();
        List<MiruPartitionedActivity> partitionedActivities = Lists.newArrayList();
        int writerId = 1;
        int numActivities = 0;
        partitionedActivities.add(factory.begin(writerId, partitionId, tenant1, numActivities));
        for (int index = 1; index <= numActivities; index++) {
            partitionedActivities.add(util.viewActivity(tenant1, partitionId, time.incrementAndGet(), "wilma",
                String.valueOf(rand.nextInt(numberOfDocument)), walIndex.incrementAndGet()));
        }

        MiruProvider<MiruService> miruProvider = new MiruPluginTestBootstrap().bootstrap(tenant1, partitionId, miruHost,
            miruSchema, MiruBackingStorage.memory, new MiruBitmapsRoaring(), partitionedActivities);

        this.service = miruProvider.getMiru(tenant1);
        this.injectable = new RecoInjectable(miruProvider, new CollaborativeFiltering(new MiruAggregateUtil(), new MiruIndexUtil()));
    }

    @Test(enabled = true)
    public void basicTest() throws Exception {
        System.out.println("Building activities....");
        long start = System.currentTimeMillis();
        int count = 0;
        int numGroups = 10;
        List<MiruPartitionedActivity> batch = new ArrayList<>();
        for (int i = 0; i < numberOfUsers; i++) {
            String user = "bob" + i;
            int randSeed = i % numGroups;
            Random userRand = new Random(randSeed * 137);
            for (int r = 0; r < 2 * (i / numGroups); r++) {
                userRand.nextInt(numberOfDocument);
            }

            for (int d = 0; d < numberOfViewsPerUser; d++) {
                int docId = userRand.nextInt(numberOfDocument);
                long activityTime = time.incrementAndGet();
                batch.add(util.viewActivity(tenant1, partitionId, activityTime, user, String.valueOf(docId), walIndex.incrementAndGet()));
                if (++count % 10_000 == 0) {
                    service.writeToIndex(batch);
                    batch.clear();
                    System.out.println("Finished " + count + " in " + (System.currentTimeMillis() - start) + " ms");
                }
            }
        }
        if (!batch.isEmpty()) {
            service.writeToIndex(batch);
            batch.clear();
        }

        System.out.println("Built and indexed " + count + " in " + (System.currentTimeMillis() - start) + "millis");

        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.incrementAndGet(), "bob0", "1",
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.incrementAndGet(), "bob0", "2",
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.incrementAndGet(), "bob0", "3",
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.incrementAndGet(), "bob0", "4",
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.incrementAndGet(), "bob0", "9",
            walIndex.incrementAndGet())));

        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.incrementAndGet(), "frank", "1",
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.incrementAndGet(), "frank", "2",
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.incrementAndGet(), "frank", "3",
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.incrementAndGet(), "frank", "4",
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.incrementAndGet(), "frank", "10",
            walIndex.incrementAndGet())));

        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.incrementAndGet(), "jane", "2",
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.incrementAndGet(), "jane", "3",
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.incrementAndGet(), "jane", "4",
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.incrementAndGet(), "jane", "11",
            walIndex.incrementAndGet())));

        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.incrementAndGet(), "liz", "3",
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.incrementAndGet(), "liz", "4",
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.incrementAndGet(), "liz", "12",
            walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.viewActivity(tenant1, partitionId, time.incrementAndGet(), "liz", "12",
            walIndex.incrementAndGet())));

        System.out.println("Running queries...");

        for (int i = 0; i < numqueries; i++) {
            String user = "bob" + i;
            MiruFieldFilter miruFieldFilter = new MiruFieldFilter(MiruFieldType.pairedLatest, "user", ImmutableList.of(
                indexUtil.makePairedLatestTerm(new MiruTermId(user.getBytes(Charsets.UTF_8)), "doc").toString()));
            MiruFilter filter = new MiruFilter(
                MiruFilterOperation.or,
                Optional.of(Arrays.asList(miruFieldFilter)),
                Optional.<List<MiruFilter>>absent());

            long s = System.currentTimeMillis();
            MiruResponse<RecoAnswer> response = injectable.collaborativeFilteringRecommendations(new MiruRequest<>(
                tenant1,
                new MiruActorId(new Id(1)),
                MiruAuthzExpression.NOT_PROVIDED,
                new RecoQuery(
                    filter,
                    "doc", "doc", "doc",
                    "user", "user", "user",
                    "doc", "doc",
                    MiruFilter.NO_FILTER,
                    10),
                MiruSolutionLogLevel.INFO));

            System.out.println("recoResult:" + response.answer.results);
            System.out.println("Took:" + (System.currentTimeMillis() - s));
            Assert.assertTrue(response.answer.results.size() > 0, response.toString());
        }

    }

}
