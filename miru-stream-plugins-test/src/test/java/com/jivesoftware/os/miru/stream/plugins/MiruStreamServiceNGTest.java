/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jivesoftware.os.miru.stream.plugins;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.miru.api.MiruActorId;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivityFactory;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFieldFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilterOperation;
import com.jivesoftware.os.miru.bitmaps.roaring5.MiruBitmapsRoaring;
import com.jivesoftware.os.miru.plugin.MiruProvider;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.plugin.test.MiruPluginTestBootstrap;
import com.jivesoftware.os.miru.service.MiruService;
import com.jivesoftware.os.miru.stream.plugins.count.DistinctCount;
import com.jivesoftware.os.miru.stream.plugins.count.DistinctCountAnswer;
import com.jivesoftware.os.miru.stream.plugins.count.DistinctCountInjectable;
import com.jivesoftware.os.miru.stream.plugins.count.DistinctCountQuery;
import com.jivesoftware.os.miru.stream.plugins.filter.AggregateCount;
import com.jivesoftware.os.miru.stream.plugins.filter.AggregateCounts;
import com.jivesoftware.os.miru.stream.plugins.filter.AggregateCountsAnswer;
import com.jivesoftware.os.miru.stream.plugins.filter.AggregateCountsInjectable;
import com.jivesoftware.os.miru.stream.plugins.filter.AggregateCountsQuery;
import com.jivesoftware.os.miru.stream.plugins.filter.AggregateCountsQueryConstraint;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * @author jonathan
 */
public class MiruStreamServiceNGTest {

    private MiruSchema miruSchema;
    private MiruFieldDefinition[] fieldDefinitions;

    MiruTenantId tenant1 = new MiruTenantId("tenant1".getBytes());
    MiruPartitionId partitionId = MiruPartitionId.of(1);
    MiruHost miruHost = new MiruHost("logicalName");

    int verb1 = 1;
    int verb2 = 2;
    int verb3 = 3;
    int container1 = 10;
    int container2 = 20;
    int container3 = 30;
    int target1 = 100;
    int target2 = 200;
    int target3 = 300;
    int tag1 = 1_000;
    int tag2 = 2_000;
    int tag3 = 3_000;
    int author1 = 10_000;
    int author2 = 20_000;
    int author3 = 30_000;

    MiruPartitionedActivityFactory partitionedActivityFactory = new MiruPartitionedActivityFactory();
    MiruService service;
    AggregateCountsInjectable aggregateCountsInjectable;
    DistinctCountInjectable distinctCountInjectable;

    @BeforeMethod
    public void setUpMethod() throws Exception {

        this.fieldDefinitions = new MiruFieldDefinition[] {
            new MiruFieldDefinition(0, "verb", MiruFieldDefinition.Type.singleTerm, MiruFieldDefinition.Prefix.NONE),
            new MiruFieldDefinition(1, "container", MiruFieldDefinition.Type.singleTerm, MiruFieldDefinition.Prefix.NONE),
            new MiruFieldDefinition(2, "target", MiruFieldDefinition.Type.singleTerm, MiruFieldDefinition.Prefix.NONE),
            new MiruFieldDefinition(3, "tag", MiruFieldDefinition.Type.singleTerm, MiruFieldDefinition.Prefix.NONE),
            new MiruFieldDefinition(4, "author", MiruFieldDefinition.Type.singleTerm, MiruFieldDefinition.Prefix.NONE)
        };
        this.miruSchema = new MiruSchema.Builder("test", 1)
            .setFieldDefinitions(fieldDefinitions)
            .build();

        MiruProvider<MiruService> miruProvider = new MiruPluginTestBootstrap().bootstrap(tenant1, partitionId, miruHost,
            miruSchema, MiruBackingStorage.disk, new MiruBitmapsRoaring(), Collections.emptyList());
        this.service = miruProvider.getMiru(tenant1);

        this.aggregateCountsInjectable = new AggregateCountsInjectable(miruProvider, new AggregateCounts(miruProvider));

        this.distinctCountInjectable = new DistinctCountInjectable(miruProvider, new DistinctCount());
    }

    @Test(enabled = false, description = "This test is disabled because it is very slow")
    public void basicTest() throws Exception {
        final int capacity = 1_000_000;
        final int numQueries = 1_000;

        DecimalFormat formatter = new DecimalFormat("###,###,###");
        Random rand = new Random(1_234);
        MiruStreamId streamId = new MiruStreamId(FilerIO.longBytes(1));
        List<MiruPartitionedActivity> activities = new ArrayList<>();
        int passes = 1;
        for (int p = 0; p < passes; p++) {
            activities.clear();

            for (int i = p * (capacity / passes); i < (p + 1) * (capacity / passes); i++) {
                activities.add(generateActivity(i, rand));
                if (i % 100_000 == 0) {

                    //System.out.println("Adding " + activities.size() + " activities.");
                    long t = System.currentTimeMillis();
                    service.writeToIndex(activities);
                    long e = (System.currentTimeMillis() - t);
                    int indexSize = p * (capacity / passes) + i;
                    System.out.println("\tIndexed " + formatter.format(activities.size()) + " activities in " + formatter.format(System.currentTimeMillis() - t)
                        + " millis ratePerSecond:" + formatter.format(1_000 * (activities.size() / e)));
                    System.out.println("\t\tIndexSize:" + formatter.format(indexSize));

                    activities.clear();
                }
            }

            if (!activities.isEmpty()) {
                long t = System.currentTimeMillis();
                service.writeToIndex(activities);
                long e = (System.currentTimeMillis() - t);
                int indexSize = (p + 1) * (capacity / passes);
                System.out.println("\tIndexed " + formatter.format(activities.size()) + " activities in " + formatter.format(System.currentTimeMillis() - t)
                    + " millis ratePerSecond:" + formatter.format(1_000 * (activities.size() / e)));
                System.out.println("\t\tIndexSize:" + formatter.format(indexSize));
            }

            //System.out.println("Adding " + activities.size() + " activities.");
            int indexSize = (p + 1) * (capacity / passes);

            for (int q = 0; q < numQueries; q++) {
                List<MiruFieldFilter> fieldFilters = new ArrayList<>();
                //fieldFilters.add(new MiruFieldFilter("author", ImmutableList.of(FilerIO.intBytes(rand.nextInt(1000)))));
                List<String> following = generateDisticts(rand, 10_000, 1_000_000);
                //System.out.println("Following:"+new MiruFieldFilter("target", ImmutableList.copyOf(following)));
                fieldFilters.add(MiruFieldFilter.of(MiruFieldType.primary, "target", following));

                MiruFilter filter = new MiruFilter(MiruFilterOperation.or, false, fieldFilters, null);
                MiruRequest<AggregateCountsQuery> query = new MiruRequest<>("test",
                    tenant1,
                    MiruActorId.NOT_PROVIDED,
                    MiruAuthzExpression.NOT_PROVIDED,
                    new AggregateCountsQuery(
                        streamId,
                        new MiruTimeRange(0, capacity),
                        new MiruTimeRange(0, capacity),
                        filter,
                        ImmutableMap.of("blah", new AggregateCountsQueryConstraint(MiruFilter.NO_FILTER,
                            "container",
                            0,
                            51,
                            new String[0]))),
                    MiruSolutionLogLevel.NONE);

                long start = System.currentTimeMillis();
                MiruResponse<AggregateCountsAnswer> results = aggregateCountsInjectable.filterInboxStreamAll(query);
                long elapse = System.currentTimeMillis() - start;
                //System.out.println("Results:" + query);
                //                for (AggregateCount a : results.results) {
                //                    System.out.println(a);
                //                }
                System.out.println("\t\t\tQuery:" + (q + 1) + " latency:" + elapse
                    + " count:" + results.answer.constraints.get("blah").results.size()
                    + " all:" + formatter.format(count(results.answer.constraints.get("blah").results))
                    + " indexSizeToLatencyRatio:" + (indexSize / elapse));
            }
        }
    }

    private long count(List<AggregateCount> results) {
        if (results == null) {
            return 0;
        }
        int count = 0;
        for (AggregateCount aggregateCount : results) {
            count += aggregateCount.count;
        }
        return count;
    }

    /**
     * schema.put("verb", 0); <br>
     * schema.put("container", 1); <br>
     * schema.put("target", 2); <br>
     * schema.put("tag", 3); <br>
     * schema.put("author", 4); <br>
     */
    private final int[] fieldCardinality = new int[] { 10, 10_000, 1_000_000, 10_000, 1_000 };
    private final int[] fieldFrequency = new int[] { 1, 1, 1, 10, 1 };

    private MiruPartitionedActivity generateActivity(int time, Random rand) {
        Map<String, List<String>> fieldsValues = Maps.newHashMap();
        for (MiruFieldDefinition fieldDefinition : fieldDefinitions) {
            int index = fieldDefinition.fieldId;
            int count = 1 + rand.nextInt(fieldFrequency[index]);
            List<String> terms = generateDisticts(rand, count, fieldCardinality[index]);
            fieldsValues.put(fieldDefinition.name, terms);
        }
        MiruActivity activity = new MiruActivity(tenant1, time, 0, false, new String[0], fieldsValues, Collections.emptyMap());
        return partitionedActivityFactory.activity(1, partitionId, 1, activity);
    }

    private List<String> generateDisticts(Random rand, int count, int cardinality) {
        Set<String> usedTerms = Sets.newHashSet();
        List<String> distincts = new ArrayList<>();
        while (distincts.size() < count) {
            String term = String.valueOf(rand.nextInt(cardinality));
            if (usedTerms.add(term)) {
                distincts.add(term);
            }
        }
        return distincts;
    }

    @Test(groups = "slow", enabled = false, description = "This test is disabled because it is very slow, enable it when you want to run it (duh)")
    public void filterTest() throws Exception {
        List<MiruPartitionedActivity> activities = new ArrayList<>();

        activities.add(buildActivity(1, verb1, container1, target1, tag1, author1));
        activities.add(buildActivity(2, verb2, container1, target1, tag1, author2));
        activities.add(buildActivity(3, verb3, container1, target1, tag1, author3));
        activities.add(buildActivity(4, verb1, container1, target2, tag1, author1));
        activities.add(buildActivity(5, verb2, container1, target2, tag1, author2));
        activities.add(buildActivity(6, verb3, container1, target2, tag1, author3));
        activities.add(buildActivity(7, verb1, container2, target3, tag3, author1));
        activities.add(buildActivity(8, verb2, container2, target3, tag3, author2));
        activities.add(buildActivity(9, verb3, container2, target3, tag3, author3));
        service.writeToIndex(activities);

        MiruStreamId streamId = new MiruStreamId(FilerIO.longBytes(1));

        List<MiruFieldFilter> fieldFilters = new ArrayList<>();
        List<String> following = new ArrayList<>();
        following.add(String.valueOf(container1));
        fieldFilters.add(MiruFieldFilter.of(MiruFieldType.primary, "container", following));
        MiruFilter followingFilter = new MiruFilter(MiruFilterOperation.or, false, fieldFilters, null);

        fieldFilters = new ArrayList<>();
        List<String> authors = new ArrayList<>();
        authors.add(String.valueOf(author1));
        fieldFilters.add(MiruFieldFilter.of(MiruFieldType.primary, "author", authors));
        MiruFilter authoredByFilter = new MiruFilter(MiruFilterOperation.or, false, fieldFilters, null);

        MiruFilter filter = new MiruFilter(MiruFilterOperation.and, false, null, Arrays.asList(followingFilter, authoredByFilter));

        //aggregateQuery:
        {
            MiruRequest<AggregateCountsQuery> query = new MiruRequest<>("test",
                tenant1,
                MiruActorId.NOT_PROVIDED,
                MiruAuthzExpression.NOT_PROVIDED, new AggregateCountsQuery(
                streamId,
                new MiruTimeRange(0, 1_000),
                new MiruTimeRange(0, 1_000),
                filter,
                ImmutableMap.of("blah", new AggregateCountsQueryConstraint(MiruFilter.NO_FILTER, "container", 0, 10, new String[0]))),
                MiruSolutionLogLevel.NONE);
            MiruResponse<AggregateCountsAnswer> results = aggregateCountsInjectable.filterInboxStreamAll(query);
            for (AggregateCount a : results.answer.constraints.get("blah").results) {
                System.out.println(a);
            }
        }

        System.out.println("--------------");

        //countQuery:
        {
            MiruRequest<DistinctCountQuery> query = new MiruRequest<>("test",
                tenant1,
                MiruActorId.NOT_PROVIDED,
                MiruAuthzExpression.NOT_PROVIDED,
                new DistinctCountQuery(
                    streamId,
                    new MiruTimeRange(0, 1_000),
                    filter,
                    MiruFilter.NO_FILTER,
                    "container",
                    50),
                MiruSolutionLogLevel.NONE);
            MiruResponse<DistinctCountAnswer> count = distinctCountInjectable.countInboxStreamAll(query);
            System.out.println(count);
        }

        activities.clear();
        activities.add(buildActivity(10, verb2, container1, target1, tag1, author1));
        service.writeToIndex(activities);

        //aggregateQuery:
        {
            MiruRequest<AggregateCountsQuery> query = new MiruRequest<>("test",
                tenant1,
                MiruActorId.NOT_PROVIDED,
                MiruAuthzExpression.NOT_PROVIDED, new AggregateCountsQuery(
                streamId,
                new MiruTimeRange(0, 1_000),
                new MiruTimeRange(0, 1_000),
                filter,
                ImmutableMap.of("blah", new AggregateCountsQueryConstraint(MiruFilter.NO_FILTER,
                    "container", 0, 10, new String[0]))),
                MiruSolutionLogLevel.NONE);
            MiruResponse<AggregateCountsAnswer> results = aggregateCountsInjectable.filterInboxStreamAll(query);
            for (AggregateCount a : results.answer.constraints.get("blah").results) {
                System.out.println(a);
            }
        }

        System.out.println("--------------");

        //countQuery:
        {
            MiruRequest<DistinctCountQuery> query = new MiruRequest<>("test",
                tenant1,
                MiruActorId.NOT_PROVIDED,
                MiruAuthzExpression.NOT_PROVIDED,
                new DistinctCountQuery(
                    streamId,
                    new MiruTimeRange(0, 1_000),
                    filter,
                    MiruFilter.NO_FILTER,
                    "container",
                    50),
                MiruSolutionLogLevel.NONE);
            MiruResponse<DistinctCountAnswer> count = distinctCountInjectable.countInboxStreamAll(query);
            System.out.println(count);
        }

    }

    private MiruPartitionedActivity buildActivity(int time, int verb, Integer container, int target, Integer tag, int author) {
        Map<String, List<String>> fieldsValues = Maps.newHashMap();
        fieldsValues.put("verb", Arrays.asList(String.valueOf(verb)));
        if (container != null) {
            fieldsValues.put("container", Arrays.asList(String.valueOf(container)));
        }
        fieldsValues.put("target", Arrays.asList(String.valueOf(target)));
        if (tag != null) {
            fieldsValues.put("tag", Arrays.asList(String.valueOf(tag)));
        }
        fieldsValues.put("author", Arrays.asList(String.valueOf(author)));
        String[] authz = new String[] { "aaabbbcccddd" };
        MiruActivity activity = new MiruActivity(tenant1, time, 0, false, authz, fieldsValues, Collections.emptyMap());
        return partitionedActivityFactory.activity(1, partitionId, 1, activity);
    }

}
