/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jivesoftware.os.miru.stream.plugins;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
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
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.bitmaps.roaring5.MiruBitmapsRoaring;
import com.jivesoftware.os.miru.plugin.MiruProvider;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.plugin.test.MiruPluginTestBootstrap;
import com.jivesoftware.os.miru.service.MiruService;
import com.jivesoftware.os.miru.stream.plugins.fulltext.FullText;
import com.jivesoftware.os.miru.stream.plugins.fulltext.FullTextAnswer;
import com.jivesoftware.os.miru.stream.plugins.fulltext.FullTextInjectable;
import com.jivesoftware.os.miru.stream.plugins.fulltext.FullTextQuery;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * @author jonathan
 */
public class MiruFullTextNGTest {

    MiruSchema miruSchema = new MiruSchema.Builder("test", 1)
        .setFieldDefinitions(new MiruFieldDefinition[] {
            new MiruFieldDefinition(0, "user", MiruFieldDefinition.Type.singleTerm, MiruFieldDefinition.Prefix.NONE),
            new MiruFieldDefinition(1, "doc", MiruFieldDefinition.Type.singleTerm, MiruFieldDefinition.Prefix.NONE),
            new MiruFieldDefinition(2, "obj", MiruFieldDefinition.Type.multiTerm,
                new MiruFieldDefinition.Prefix(MiruFieldDefinition.Prefix.Type.numeric, 4, ' ')),
            new MiruFieldDefinition(3, "text", MiruFieldDefinition.Type.multiTermCardinality, MiruFieldDefinition.Prefix.WILDCARD)
        })
        .build();

    MiruTenantId tenant1 = new MiruTenantId("tenant1".getBytes());
    MiruPartitionId partitionId = MiruPartitionId.of(1);
    MiruHost miruHost = new MiruHost("logicalName");
    AtomicInteger walIndex = new AtomicInteger();

    private final int numberOfQueries = 2;
    private final int numberOfTermsPerQuery = 10;
    private final int numberOfUsers = 10;
    private final int numberOfDocsPerUser = 2;
    private final int numberOfTermsPerDoc = 100;
    private final int numberOfActivities = numberOfUsers * numberOfDocsPerUser;
    private final int numberOfBuckets = 32;

    private MiruService service;
    private FullTextInjectable injectable;

    private final MiruPartitionedActivityFactory partitionedActivityFactory = new MiruPartitionedActivityFactory();
    private final String[] dictionary = new String[10];

    @BeforeMethod
    public void setUpMethod() throws Exception {
        MiruProvider<MiruService> miruProvider = new MiruPluginTestBootstrap().bootstrap(tenant1, partitionId, miruHost,
            miruSchema, MiruBackingStorage.disk, new MiruBitmapsRoaring(), Collections.emptyList());

        this.service = miruProvider.getMiru(tenant1);
        this.injectable = new FullTextInjectable(miruProvider, new FullText(miruProvider));

        for (int i = 0; i < dictionary.length; i++) {
            dictionary[i] = Integer.toHexString(i);
        }
    }

    @Test(enabled = true)
    public void basicTest() throws Exception {

        Random rand = new Random();
        SnowflakeIdPacker snowflakeIdPacker = new SnowflakeIdPacker();
        long timespan = numberOfBuckets * snowflakeIdPacker.pack(TimeUnit.HOURS.toMillis(3), 0, 0);
        long intervalPerActivity = Math.max(timespan / numberOfActivities, 1);
        AtomicLong time = new AtomicLong(snowflakeIdPacker.pack(System.currentTimeMillis(), 0, 0) - timespan);
        System.out.println("Building activities....");

        service.writeToIndex(Collections.singletonList(contentActivity(rand, tenant1, partitionId, time.addAndGet(intervalPerActivity),
            "bob0", walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(contentActivity(rand, tenant1, partitionId, time.addAndGet(intervalPerActivity),
            "bob0", walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(contentActivity(rand, tenant1, partitionId, time.addAndGet(intervalPerActivity),
            "bob0", walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(contentActivity(rand, tenant1, partitionId, time.addAndGet(intervalPerActivity),
            "bob0", walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(contentActivity(rand, tenant1, partitionId, time.addAndGet(intervalPerActivity),
            "bob0", walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(contentActivity(rand, tenant1, partitionId, time.addAndGet(intervalPerActivity),
            "bob0", walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(contentActivity(rand, tenant1, partitionId, time.addAndGet(intervalPerActivity),
            "bob0", walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(contentActivity(rand, tenant1, partitionId, time.addAndGet(intervalPerActivity),
            "bob0", walIndex.incrementAndGet())));

        service.writeToIndex(Collections.singletonList(contentActivity(rand, tenant1, partitionId, time.addAndGet(intervalPerActivity),
            "frank", walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(contentActivity(rand, tenant1, partitionId, time.addAndGet(intervalPerActivity),
            "frank", walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(contentActivity(rand, tenant1, partitionId, time.addAndGet(intervalPerActivity),
            "frank", walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(contentActivity(rand, tenant1, partitionId, time.addAndGet(intervalPerActivity),
            "frank", walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(contentActivity(rand, tenant1, partitionId, time.addAndGet(intervalPerActivity),
            "frank", walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(contentActivity(rand, tenant1, partitionId, time.addAndGet(intervalPerActivity),
            "frank", walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(contentActivity(rand, tenant1, partitionId, time.addAndGet(intervalPerActivity),
            "frank", walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(contentActivity(rand, tenant1, partitionId, time.addAndGet(intervalPerActivity),
            "frank", walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(contentActivity(rand, tenant1, partitionId, time.addAndGet(intervalPerActivity),
            "frank", walIndex.incrementAndGet())));

        service.writeToIndex(Collections.singletonList(contentActivity(rand, tenant1, partitionId, time.addAndGet(intervalPerActivity),
            "jane", walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(contentActivity(rand, tenant1, partitionId, time.addAndGet(intervalPerActivity),
            "jane", walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(contentActivity(rand, tenant1, partitionId, time.addAndGet(intervalPerActivity),
            "jane", walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(contentActivity(rand, tenant1, partitionId, time.addAndGet(intervalPerActivity),
            "jane", walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(contentActivity(rand, tenant1, partitionId, time.addAndGet(intervalPerActivity),
            "jane", walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(contentActivity(rand, tenant1, partitionId, time.addAndGet(intervalPerActivity),
            "jane", walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(contentActivity(rand, tenant1, partitionId, time.addAndGet(intervalPerActivity),
            "jane", walIndex.incrementAndGet())));

        service.writeToIndex(Collections.singletonList(contentActivity(rand, tenant1, partitionId, time.addAndGet(intervalPerActivity),
            "liz", walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(contentActivity(rand, tenant1, partitionId, time.addAndGet(intervalPerActivity),
            "liz", walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(contentActivity(rand, tenant1, partitionId, time.addAndGet(intervalPerActivity),
            "liz", walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(contentActivity(rand, tenant1, partitionId, time.addAndGet(intervalPerActivity),
            "liz", walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(contentActivity(rand, tenant1, partitionId, time.addAndGet(intervalPerActivity),
            "liz", walIndex.incrementAndGet())));
        service.writeToIndex(Collections.singletonList(contentActivity(rand, tenant1, partitionId, time.addAndGet(intervalPerActivity),
            "liz", walIndex.incrementAndGet())));

        List<MiruPartitionedActivity> batch = Lists.newArrayListWithCapacity(10_000);
        for (int i = 0; i < numberOfActivities; i++) {
            batch.add(contentActivity(rand, tenant1, partitionId, time.addAndGet(intervalPerActivity),
                "bob" + rand.nextInt(numberOfUsers), walIndex.incrementAndGet()));
            if (batch.size() == 10_000) {
                System.out.println("Indexing " + (i + 1) + " / " + numberOfActivities + " activities");
                service.writeToIndex(batch);
                batch.clear();
            }
        }
        if (!batch.isEmpty()) {
            service.writeToIndex(batch);
            batch.clear();
        }

        System.out.println("Indexed " + numberOfActivities + " / " + numberOfActivities + " activities");
        System.out.println("Running queries...");

        runQueries(rand, timespan, time.get(), FullTextQuery.Strategy.TIME);
        runQueries(rand, timespan, time.get(), FullTextQuery.Strategy.TF_IDF);
    }

    private void runQueries(Random rand, long timespan, long lastTime, FullTextQuery.Strategy strategy) throws MiruQueryServiceException, InterruptedException {
        System.out.println("-------- " + strategy + " --------");
        final MiruTimeRange timeRange = new MiruTimeRange(lastTime - timespan, lastTime);

        for (int i = 0; i < numberOfQueries; i++) {
            long s = System.currentTimeMillis();
            MiruRequest<FullTextQuery> request = new MiruRequest<>("test",
                tenant1,
                MiruActorId.NOT_PROVIDED,
                MiruAuthzExpression.NOT_PROVIDED,
                new FullTextQuery(timeRange,
                    "text",
                    "en",
                    queryAnd(rand, numberOfTermsPerQuery),
                    -1,
                    MiruFilter.NO_FILTER,
                    strategy,
                    100,
                    new String[0]),
                MiruSolutionLogLevel.INFO);
            MiruResponse<FullTextAnswer> fullTextResult = injectable.filterCustomStream(request);

            float minScore = Float.MAX_VALUE;
            float maxScore = -Float.MAX_VALUE;
            for (FullTextAnswer.ActivityScore result : fullTextResult.answer.results) {
                minScore = Math.min(minScore, result.score);
                maxScore = Math.max(maxScore, result.score);
            }

            assertFalse(fullTextResult.answer.results.isEmpty());
            if (strategy == FullTextQuery.Strategy.TF_IDF) {
                assertTrue(minScore > 0f);
                assertTrue(maxScore < 1f);
            } else {
                assertEquals(minScore, 0f);
                assertEquals(maxScore, 0f);
            }

            System.out.println("fullTextResult: size: " + fullTextResult.answer.results.size() + " elapsed: " + (System.currentTimeMillis() - s));
            //System.out.println("fullTextResult: minScore: " + minScore);
            //System.out.println("fullTextResult: maxScore: " + maxScore);
        }
    }

    public MiruPartitionedActivity contentActivity(Random rand,
        MiruTenantId tenantId,
        MiruPartitionId partitionId,
        long time,
        String user,
        int index) {

        String doc = String.valueOf(index);
        int hash = Math.abs(doc.hashCode());
        List<String> text = Lists.newArrayListWithCapacity(numberOfTermsPerDoc);
        for (int i = 0; i < numberOfTermsPerDoc; i++) {
            text.add(dictionary[rand.nextInt(dictionary.length)]);
        }

        Map<String, List<String>> fieldsValues = Maps.newHashMap();
        fieldsValues.put("user", Arrays.asList(user));
        fieldsValues.put("doc", Arrays.asList(doc));
        fieldsValues.put("obj", Arrays.asList((hash % 4) + " " + doc));
        fieldsValues.put("text", text);

        MiruActivity activity = new MiruActivity(tenantId, time, 0, false, new String[0], fieldsValues, Collections.emptyMap());
        return partitionedActivityFactory.activity(1, partitionId, index, activity);
    }

    private String queryAnd(Random rand, int clauses) {
        StringBuilder buf = new StringBuilder();
        for (int i = 0; i < clauses; i++) {
            if (i > 0) {
                buf.append(" AND ");
            }
            buf.append(dictionary[rand.nextInt(dictionary.length)]);
        }
        return buf.toString();
    }
}
