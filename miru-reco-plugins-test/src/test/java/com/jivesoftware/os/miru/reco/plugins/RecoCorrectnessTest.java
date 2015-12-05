package com.jivesoftware.os.miru.reco.plugins;

import com.google.common.base.Charsets;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.jivesoftware.os.miru.analytics.plugins.analytics.Analytics;
import com.jivesoftware.os.miru.api.MiruActorId;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
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
import com.jivesoftware.os.miru.bitmaps.roaring5.buffer.MiruBitmapsRoaringBuffer;
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
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsQuery;
import com.jivesoftware.os.miru.reco.plugins.reco.CollaborativeFiltering;
import com.jivesoftware.os.miru.reco.plugins.reco.RecoAnswer;
import com.jivesoftware.os.miru.reco.plugins.reco.RecoInjectable;
import com.jivesoftware.os.miru.reco.plugins.reco.RecoQuery;
import com.jivesoftware.os.miru.reco.plugins.trending.TrendingAnswer;
import com.jivesoftware.os.miru.reco.plugins.trending.TrendingInjectable;
import com.jivesoftware.os.miru.reco.plugins.trending.TrendingQuery;
import com.jivesoftware.os.miru.reco.plugins.trending.TrendingQuery.Strategy;
import com.jivesoftware.os.miru.reco.plugins.trending.Trendy;
import com.jivesoftware.os.miru.service.MiruService;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition.Type.multiTerm;
import static com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition.Type.singleTerm;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * @author jonathan
 */
public class RecoCorrectnessTest {

    MiruInterner<MiruTermId> termInterner = new MiruInterner<MiruTermId>(true) {
        @Override
        public MiruTermId create(byte[] bytes) {
            return new MiruTermId(bytes);
        }
    };

    final MiruFieldDefinition.Prefix TYPED_PREFIX = new MiruFieldDefinition.Prefix(MiruFieldDefinition.Prefix.Type.numeric, 4, ' ');

    MiruSchema miruSchema = new MiruSchema.Builder("reco", 1)
        .setFieldDefinitions(new MiruFieldDefinition[]{
        new MiruFieldDefinition(0, "locale", singleTerm, MiruFieldDefinition.Prefix.NONE),
        new MiruFieldDefinition(1, "mode", singleTerm, MiruFieldDefinition.Prefix.NONE),
        new MiruFieldDefinition(2, "activityType", singleTerm, MiruFieldDefinition.Prefix.NONE),
        new MiruFieldDefinition(3, "contextType", singleTerm, MiruFieldDefinition.Prefix.NONE),
        new MiruFieldDefinition(4, "context", singleTerm, TYPED_PREFIX),
        new MiruFieldDefinition(5, "objectType", singleTerm, MiruFieldDefinition.Prefix.NONE),
        new MiruFieldDefinition(6, "object", singleTerm, TYPED_PREFIX),
        new MiruFieldDefinition(7, "parentType", singleTerm, MiruFieldDefinition.Prefix.NONE),
        new MiruFieldDefinition(8, "parent", singleTerm, TYPED_PREFIX),
        new MiruFieldDefinition(9, "user", singleTerm, MiruFieldDefinition.Prefix.NONE),
        new MiruFieldDefinition(10, "authors", multiTerm, MiruFieldDefinition.Prefix.NONE)
    })
        .setPairedLatest(ImmutableMap.of(
            "parent", Arrays.asList("user"),
            "user", Arrays.asList("parent", "context", "user")))
        .setBloom(ImmutableMap.of(
            "context", Arrays.asList("user"),
            "parent", Arrays.asList("user"),
            "user", Arrays.asList("user")))
        .build();

    MiruTermComposer termComposer = new MiruTermComposer(Charsets.UTF_8, termInterner);
    MiruTenantId tenant1 = new MiruTenantId("tenant1".getBytes());
    MiruPartitionId partitionId = MiruPartitionId.of(1);
    MiruHost miruHost = new MiruHost("logicalName", 1_234);
    CollaborativeFilterUtil util = new CollaborativeFilterUtil();
    MiruAggregateUtil aggregateUtil = new MiruAggregateUtil();
    MiruIndexUtil indexUtil = new MiruIndexUtil();
    AtomicLong time = new AtomicLong();
    AtomicInteger walIndex = new AtomicInteger();

    int numqueries = 1_000;
    int numberOfUsers = 100;
    int numberOfDocument = 10_000;
    int numberOfViewsPerUser = 1_000;

    boolean doSystemRecommendedContent = false;
    boolean doContainerTrendingContent = true;

    MiruService service;
    RecoInjectable recoInjectable;
    TrendingInjectable trendingInjectable;

    @BeforeMethod
    public void setUpMethod() throws Exception {
        MiruPartitionedActivityFactory factory = new MiruPartitionedActivityFactory();
        List<MiruPartitionedActivity> partitionedActivities = Lists.newArrayList();
        int writerId = 1;
        partitionedActivities.add(factory.begin(writerId, partitionId, tenant1, 0));

        MiruProvider<MiruService> miruProvider = new MiruPluginTestBootstrap().bootstrap(tenant1, partitionId, miruHost,
            miruSchema, MiruBackingStorage.disk, new MiruBitmapsRoaringBuffer(), partitionedActivities);

        this.service = miruProvider.getMiru(tenant1);

        this.recoInjectable = new RecoInjectable(miruProvider, new CollaborativeFiltering(aggregateUtil, indexUtil), new Distincts(termComposer));
        this.trendingInjectable = new TrendingInjectable(miruProvider, new Distincts(termComposer), new Analytics());
    }

    @Test(enabled = false)
    public void basicTest() throws Exception {
        System.out.println("Building activities....");
        long start = System.currentTimeMillis();
        int count = 0;
        int numGroups = 10;
        List<MiruPartitionedActivity> batch = new ArrayList<>();
        SetMultimap<String, String> authorToParents = HashMultimap.create();
        SetMultimap<String, String> userToParents = HashMultimap.create();
        SetMultimap<String, String> contextToParents = HashMultimap.create();
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

                int contextType = 100 + docId % 3;
                String context = contextType + " place-" + (docId % 10);

                int parentType = 50 + docId % 6;
                String parent = parentType + " doc-" + docId;

                Random authorRand = new Random(docId * 137);
                String author1 = "bob" + authorRand.nextInt(numberOfUsers);
                String author2 = "bob" + authorRand.nextInt(numberOfUsers);
                String author3 = "bob" + authorRand.nextInt(numberOfUsers);
                List<String> authors = Arrays.asList(author1, author2, author3);

                batch.add(viewActivity(tenant1, partitionId, activityTime, user, contextType, context, parentType, parent,
                    authors, walIndex.incrementAndGet()));

                userToParents.put(user, parent);
                authorToParents.put(author1, parent);
                authorToParents.put(author2, parent);
                authorToParents.put(author3, parent);
                contextToParents.put(context, parent);

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

        MiruTimeRange timeRange = new MiruTimeRange(0, time.get() + 1);

        System.out.println("Built and indexed " + count + " in " + (System.currentTimeMillis() - start) + "millis");

        if (doSystemRecommendedContent) {
            System.out.println("Running system recommended content...");
            testSystemRecommendedContent(authorToParents, userToParents, timeRange);
        }
        if (doContainerTrendingContent) {
            System.out.println("Running container trending content...");
            testContainerTrendingContent(contextToParents, timeRange);
        }
    }

    private void testSystemRecommendedContent(SetMultimap<String, String> authorToParents, SetMultimap<String, String> userToParents, MiruTimeRange timeRange)
        throws MiruQueryServiceException, InterruptedException {

        Set<String> docTypes = Sets.newHashSet("50", "51", "52");
        MiruFieldDefinition userFieldDefinition = miruSchema.getFieldDefinition(miruSchema.getFieldId("user"));
        for (int i = 0; i < numqueries; i++) {
            String user = "bob" + i;
            MiruFieldFilter miruFieldFilter = new MiruFieldFilter(MiruFieldType.pairedLatest, "user", ImmutableList.of(
                indexUtil.makePairedLatestTerm(termComposer.compose(userFieldDefinition, user), "parent").toString()));
            MiruFilter filter = new MiruFilter(MiruFilterOperation.or, false, Arrays.asList(miruFieldFilter), null);

            long s = System.currentTimeMillis();
            MiruResponse<RecoAnswer> response = recoInjectable.collaborativeFilteringRecommendations(new MiruRequest<>("test",
                tenant1,
                MiruActorId.NOT_PROVIDED,
                MiruAuthzExpression.NOT_PROVIDED,
                new RecoQuery(
                    timeRange,
                    null,
                    filter,
                    "parent", "parent", "parent",
                    "user", "user", "user",
                    "parent", "parent",
                    new MiruFilter(MiruFilterOperation.pButNotQ,
                        false,
                        null,
                        Arrays.asList(
                            new MiruFilter(MiruFilterOperation.and,
                                false,
                                Arrays.asList(
                                    new MiruFieldFilter(MiruFieldType.primary, "activityType", Arrays.asList("0", "1", "72", "65")),
                                    new MiruFieldFilter(MiruFieldType.primary, "parentType", Lists.newArrayList(docTypes))),
                                null),
                            new MiruFilter(MiruFilterOperation.and,
                                false,
                                Arrays.asList(
                                    new MiruFieldFilter(MiruFieldType.primary, "authors", Arrays.asList(user))),
                                null))),
                    10),
                MiruSolutionLogLevel.INFO));

            System.out.println("recoResult:" + response.answer.results);
            System.out.println("Took:" + (System.currentTimeMillis() - s));
            //assertTrue(response.answer.results.size() > 0, response.toString());
            for (RecoAnswer.Recommendation result : response.answer.results) {
                assertTrue(docTypes.contains(result.distinctValue.substring(0, result.distinctValue.indexOf(' '))), "Didn't expect " + result.distinctValue);
                assertFalse(authorToParents.containsEntry(user, result.distinctValue));
                assertFalse(userToParents.containsEntry(user, result.distinctValue));
            }
        }
    }

    private void testContainerTrendingContent(SetMultimap<String, String> contextToParents,
        MiruTimeRange timeRange) throws MiruQueryServiceException, InterruptedException {

        Random rand = new Random(1_234);
        Set<String> docTypes = Sets.newHashSet("50", "51", "52");
        String[] contexts = contextToParents.keySet().toArray(new String[contextToParents.keySet().size()]);
        for (int i = 0; i < numqueries; i++) {
            String context = contexts[rand.nextInt(contexts.length)];
            MiruFilter constraintsFilter = new MiruFilter(MiruFilterOperation.and,
                false,
                Arrays.asList(
                    new MiruFieldFilter(MiruFieldType.primary, "context", Arrays.asList(context)),
                    new MiruFieldFilter(MiruFieldType.primary, "parentType", Lists.newArrayList(docTypes)),
                    new MiruFieldFilter(MiruFieldType.primary, "activityType", Arrays.asList("0", "1", "72", "65"))
                ),
                null);

            long s = System.currentTimeMillis();
            MiruResponse<TrendingAnswer> response = trendingInjectable.scoreTrending(new MiruRequest<>("test",
                tenant1,
                MiruActorId.NOT_PROVIDED,
                MiruAuthzExpression.NOT_PROVIDED,
                new TrendingQuery(Collections.singleton(Strategy.LINEAR_REGRESSION),
                    timeRange,
                    null,
                    27,
                    constraintsFilter,
                    "parent",
                    Collections.singletonList(new DistinctsQuery(
                        timeRange,
                        "parent",
                        MiruFilter.NO_FILTER,
                        Lists.newArrayList(docTypes))),
                    10),
                MiruSolutionLogLevel.INFO));

            System.out.println("trendingResult:" + response.answer.results);
            System.out.println("Took:" + (System.currentTimeMillis() - s));
            //assertTrue(response.answer.results.size() > 0, response.toString());
            for (Trendy result : response.answer.results.get(Strategy.LINEAR_REGRESSION.name())) {
                assertTrue(docTypes.contains(result.distinctValue.substring(0, result.distinctValue.indexOf(' '))), "Didn't expect " + result.distinctValue);
                assertTrue(contextToParents.get(context).contains(result.distinctValue));
            }
        }
    }

    private MiruPartitionedActivityFactory partitionedActivityFactory = new MiruPartitionedActivityFactory();

    private MiruPartitionedActivity viewActivity(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        long time,
        String user,
        int contextType,
        String context,
        int parentType,
        String parent,
        List<String> authors,
        int index) {

        Map<String, List<String>> fieldsValues = Maps.newHashMap();
        fieldsValues.put("locale", Arrays.asList("en"));
        fieldsValues.put("mode", Arrays.asList("LIVE"));
        fieldsValues.put("activityType", Arrays.asList("0"));
        fieldsValues.put("contextType", Arrays.asList(String.valueOf(contextType)));
        fieldsValues.put("context", Arrays.asList(context));
        fieldsValues.put("objectType", Arrays.asList(String.valueOf(parentType)));
        fieldsValues.put("object", Arrays.asList(parent));
        fieldsValues.put("parentType", Arrays.asList(String.valueOf(parentType)));
        fieldsValues.put("parent", Arrays.asList(parent));
        fieldsValues.put("user", Arrays.asList(user));
        fieldsValues.put("authors", authors);

        MiruActivity activity = new MiruActivity(tenantId, time, new String[0], 0, fieldsValues, Collections.<String, List<String>>emptyMap());
        return partitionedActivityFactory.activity(1, partitionId, index, activity);
    }

}
