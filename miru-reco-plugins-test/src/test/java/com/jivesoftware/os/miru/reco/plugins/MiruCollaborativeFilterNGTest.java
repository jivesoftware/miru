package com.jivesoftware.os.miru.reco.plugins;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
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
import com.jivesoftware.os.miru.bitmaps.roaring6.MiruBitmapsRoaring;
import com.jivesoftware.os.miru.plugin.MiruInterner;
import com.jivesoftware.os.miru.plugin.MiruProvider;
import com.jivesoftware.os.miru.plugin.index.MiruIndexUtil;
import com.jivesoftware.os.miru.plugin.index.MiruTermComposer;
import com.jivesoftware.os.miru.plugin.solution.MiruAggregateUtil;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.plugin.test.MiruPluginTestBootstrap;
import com.jivesoftware.os.miru.reco.plugins.distincts.Distincts;
import com.jivesoftware.os.miru.reco.plugins.reco.CollaborativeFiltering;
import com.jivesoftware.os.miru.reco.plugins.reco.RecoAnswer;
import com.jivesoftware.os.miru.reco.plugins.reco.RecoInjectable;
import com.jivesoftware.os.miru.reco.plugins.reco.RecoQuery;
import com.jivesoftware.os.miru.service.MiruService;
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

    MiruInterner<MiruTermId> termInterner = new MiruInterner<MiruTermId>(true) {
        @Override
        public MiruTermId create(byte[] bytes) {
            return new MiruTermId(bytes);
        }
    };

    MiruSchema miruSchema = new MiruSchema.Builder("test", 1)
        .setFieldDefinitions(new MiruFieldDefinition[] {
            new MiruFieldDefinition(0, "user", MiruFieldDefinition.Type.singleTerm, MiruFieldDefinition.Prefix.NONE),
            new MiruFieldDefinition(1, "doc", MiruFieldDefinition.Type.singleTerm, MiruFieldDefinition.Prefix.NONE),
            new MiruFieldDefinition(2, "docType", MiruFieldDefinition.Type.singleTerm, MiruFieldDefinition.Prefix.NONE),
            new MiruFieldDefinition(3, "activityType", MiruFieldDefinition.Type.singleTerm, MiruFieldDefinition.Prefix.NONE)
        })
        .build();

    MiruTermComposer termComposer = new MiruTermComposer(Charsets.UTF_8, termInterner);
    MiruTenantId tenant1 = new MiruTenantId("tenant1".getBytes());
    MiruPartitionId partitionId = MiruPartitionId.of(1);
    MiruHost miruHost = new MiruHost("logicalName");
    CollaborativeFilterUtil util = new CollaborativeFilterUtil();
    MiruIndexUtil indexUtil = new MiruIndexUtil();
    AtomicInteger time = new AtomicInteger();
    AtomicInteger walIndex = new AtomicInteger();
    Random rand = new Random(1_234);

    int numqueries = 2;
    int numberOfUsers = 2;
    int numberOfDocument = 1000;
    int numberOfViewsPerUser = 100;
    int numberOfActivityType = 10;

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
            partitionedActivities.add(util.typedActivity(tenant1, partitionId, time.incrementAndGet(), "wilma",
                String.valueOf(rand.nextInt(numberOfDocument)), String.valueOf(rand.nextInt(numberOfActivityType)), walIndex.incrementAndGet()));
        }

        MiruProvider<MiruService> miruProvider = new MiruPluginTestBootstrap().bootstrap(tenant1, partitionId, miruHost,
            miruSchema, MiruBackingStorage.disk, new MiruBitmapsRoaring(), partitionedActivities);

        this.service = miruProvider.getMiru(tenant1);
        this.injectable = new RecoInjectable(miruProvider,
            new CollaborativeFiltering(new MiruAggregateUtil(), new MiruIndexUtil()),
            new Distincts(termComposer));
    }

    @Test(enabled = true)
    public void basicTest() throws Exception {
        System.out.println("Building activities....");
        final long start = System.currentTimeMillis();
        int count = 0;
        final int numGroups = 10;
        final List<MiruPartitionedActivity> batch = new ArrayList<>();
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
                batch.add(util.typedActivity(tenant1, partitionId, activityTime, user, String.valueOf(docId),
                    String.valueOf(rand.nextInt(numberOfActivityType)), walIndex.incrementAndGet()));
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

        service.writeToIndex(Collections.singletonList(util.typedActivity(tenant1, partitionId, time.incrementAndGet(), "bob0", "1",
            String.valueOf(rand.nextInt(numberOfActivityType)), walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.typedActivity(tenant1, partitionId, time.incrementAndGet(), "bob0", "2",
            String.valueOf(rand.nextInt(numberOfActivityType)), walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.typedActivity(tenant1, partitionId, time.incrementAndGet(), "bob0", "3",
            String.valueOf(rand.nextInt(numberOfActivityType)), walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.typedActivity(tenant1, partitionId, time.incrementAndGet(), "bob0", "4",
            String.valueOf(rand.nextInt(numberOfActivityType)), walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.typedActivity(tenant1, partitionId, time.incrementAndGet(), "bob0", "9",
            String.valueOf(rand.nextInt(numberOfActivityType)), walIndex.incrementAndGet())));

        service.writeToIndex(Collections.singletonList(util.typedActivity(tenant1, partitionId, time.incrementAndGet(), "frank", "1",
            String.valueOf(rand.nextInt(numberOfActivityType)), walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.typedActivity(tenant1, partitionId, time.incrementAndGet(), "frank", "2",
            String.valueOf(rand.nextInt(numberOfActivityType)), walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.typedActivity(tenant1, partitionId, time.incrementAndGet(), "frank", "3",
            String.valueOf(rand.nextInt(numberOfActivityType)), walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.typedActivity(tenant1, partitionId, time.incrementAndGet(), "frank", "4",
            String.valueOf(rand.nextInt(numberOfActivityType)), walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.typedActivity(tenant1, partitionId, time.incrementAndGet(), "frank", "10",
            String.valueOf(rand.nextInt(numberOfActivityType)), walIndex.incrementAndGet())));

        service.writeToIndex(Collections.singletonList(util.typedActivity(tenant1, partitionId, time.incrementAndGet(), "jane", "2",
            String.valueOf(rand.nextInt(numberOfActivityType)), walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.typedActivity(tenant1, partitionId, time.incrementAndGet(), "jane", "3",
            String.valueOf(rand.nextInt(numberOfActivityType)), walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.typedActivity(tenant1, partitionId, time.incrementAndGet(), "jane", "4",
            String.valueOf(rand.nextInt(numberOfActivityType)), walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.typedActivity(tenant1, partitionId, time.incrementAndGet(), "jane", "11",
            String.valueOf(rand.nextInt(numberOfActivityType)), walIndex.incrementAndGet())));

        service.writeToIndex(Collections.singletonList(util.typedActivity(tenant1, partitionId, time.incrementAndGet(), "liz", "3",
            String.valueOf(rand.nextInt(numberOfActivityType)), walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.typedActivity(tenant1, partitionId, time.incrementAndGet(), "liz", "4",
            String.valueOf(rand.nextInt(numberOfActivityType)), walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.typedActivity(tenant1, partitionId, time.incrementAndGet(), "liz", "12",
            String.valueOf(rand.nextInt(numberOfActivityType)), walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(util.typedActivity(tenant1, partitionId, time.incrementAndGet(), "liz", "12",
            String.valueOf(rand.nextInt(numberOfActivityType)), walIndex.incrementAndGet())));

        System.out.println("Running queries...");

        for (int i = 0; i < numqueries; i++) {
            String user = "bob" + (i % numberOfUsers);

            MiruFieldFilter userFieldFilter = MiruFieldFilter.ofTerms(MiruFieldType.primary, "user", user);
            MiruFilter myActivityFilter = new MiruFilter(MiruFilterOperation.and, false, Collections.singletonList(userFieldFilter), null);

            MiruFieldFilter docTypesFieldFilter = MiruFieldFilter.ofTerms(MiruFieldType.primary, "docType", 0, 1, 2, 3);
            MiruFieldFilter activityTypesFieldFilter = MiruFieldFilter.ofTerms(MiruFieldType.primary, "activityType", 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
            MiruFilter scorableFilter = new MiruFilter(MiruFilterOperation.and, false, Arrays.asList(docTypesFieldFilter, activityTypesFieldFilter), null);

            long s = System.currentTimeMillis();
            MiruResponse<RecoAnswer> response = injectable.collaborativeFilteringRecommendations(new MiruRequest<>("test",
                tenant1,
                MiruActorId.NOT_PROVIDED,
                MiruAuthzExpression.NOT_PROVIDED,
                new RecoQuery(
                    new MiruTimeRange(0, time.get() + 1),
                    null,
                    myActivityFilter,
                    "doc",
                    "user",
                    "doc",
                    scorableFilter,
                    10),
                MiruSolutionLogLevel.INFO));

            System.out.println("recoResult:" + response.answer.results);
            System.out.println("Took:" + (System.currentTimeMillis() - s));
            Assert.assertTrue(response.answer.results.size() > 0, response.toString());
        }

    }

}
