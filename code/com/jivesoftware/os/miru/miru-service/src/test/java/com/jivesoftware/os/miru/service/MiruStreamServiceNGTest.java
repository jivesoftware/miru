/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jivesoftware.os.miru.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.jivesoftware.os.jive.utils.http.client.HttpClientConfiguration;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactory;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactoryProvider;
import com.jivesoftware.os.jive.utils.io.FilerIO;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionCoordMetrics;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivityFactory;
import com.jivesoftware.os.miru.api.base.MiruIBA;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.query.AggregateCountsQuery;
import com.jivesoftware.os.miru.api.query.DistinctCountQuery;
import com.jivesoftware.os.miru.api.query.MiruTimeRange;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFieldFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilterOperation;
import com.jivesoftware.os.miru.api.query.result.AggregateCountsResult;
import com.jivesoftware.os.miru.api.query.result.AggregateCountsResult.AggregateCount;
import com.jivesoftware.os.miru.api.query.result.DistinctCountResult;
import com.jivesoftware.os.miru.cluster.MiruActivityLookupTable;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.cluster.memory.MiruInMemoryClusterRegistry;
import com.jivesoftware.os.miru.cluster.naive.MiruNoOpActivityLookupTable;
import com.jivesoftware.os.miru.service.partition.MiruExpectedTenants;
import com.jivesoftware.os.miru.service.partition.MiruHostedPartition;
import com.jivesoftware.os.miru.service.partition.MiruHostedPartitionComparison;
import com.jivesoftware.os.miru.service.partition.MiruPartitionDirector;
import com.jivesoftware.os.miru.service.partition.MiruPartitionEventHandler;
import com.jivesoftware.os.miru.service.partition.cluster.MiruClusterExpectedTenants;
import com.jivesoftware.os.miru.service.schema.MiruSchema;
import com.jivesoftware.os.miru.service.stream.MiruStreamFactory;
import com.jivesoftware.os.miru.service.stream.locator.MiruTempDirectoryResourceLocator;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALWriter;
import com.jivesoftware.os.miru.wal.activity.naive.MiruNoOpActivityWAL;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** @author jonathan */
public class MiruStreamServiceNGTest {

    MiruTenantId tenant1 = new MiruTenantId("tenant1".getBytes());
    int verb1 = 1;
    int verb2 = 2;
    int verb3 = 3;
    int container1 = 10;
    int container2 = 20;
    int container3 = 30;
    int target1 = 100;
    int target2 = 200;
    int target3 = 300;
    int tag1 = 1000;
    int tag2 = 2000;
    int tag3 = 3000;
    int author1 = 10000;
    int author2 = 20000;
    int author3 = 30000;

    int capacity = 100_000;

    MiruPartitionedActivityFactory partitionedActivityFactory = new MiruPartitionedActivityFactory();
    Map<String, Integer> rawSchema = new HashMap<>();
    MiruService service;
    MiruPartitionId partitionId = MiruPartitionId.of(1);

    @BeforeMethod
    public void setUpMethod() throws Exception {

        rawSchema.put("verb", 0);
        rawSchema.put("container", 1);
        rawSchema.put("target", 2);
        rawSchema.put("tag", 3);
        rawSchema.put("author", 4);

        MiruServiceConfig config = mock(MiruServiceConfig.class);
        when(config.getBitsetBufferSize()).thenReturn(8192);
        when(config.getHeartbeatIntervalInMillis()).thenReturn(10L);
        when(config.getEnsurePartitionsIntervalInMillis()).thenReturn(10L);
        when(config.getPartitionBootstrapIntervalInMillis()).thenReturn(10L);
        when(config.getPartitionRunnableIntervalInMillis()).thenReturn(10L);
        when(config.getDefaultInitialSolvers()).thenReturn(1);
        when(config.getDefaultMaxNumberOfSolvers()).thenReturn(1);
        when(config.getDefaultAddAnotherSolverAfterNMillis()).thenReturn(1000L);
        when(config.getDefaultFailAfterNMillis()).thenReturn(60000L);
        MiruReadTrackingWALReader miruReadTrackingWALReader = mock(MiruReadTrackingWALReader.class);

        MiruHost miruHost = new MiruHost("logicalName", 1234);
        MiruPartitionCoord coord = new MiruPartitionCoord(tenant1, partitionId, miruHost);
        MiruStreamFactory factory = new MiruStreamFactory(config, new MiruSchema(ImmutableMap.copyOf(rawSchema)), Executors.newCachedThreadPool(),
            miruReadTrackingWALReader, new MiruTempDirectoryResourceLocator(), new MiruTempDirectoryResourceLocator());

        MiruInMemoryClusterRegistry clusterRegistry = new MiruInMemoryClusterRegistry();
        clusterRegistry.addTenantHosts(tenant1, Collections.singletonList(miruHost));
        clusterRegistry.addTenantCoords(tenant1, Collections.singletonList(coord));
        clusterRegistry.refreshTopology(coord, new MiruPartitionCoordMetrics(0, 0), System.currentTimeMillis());

        MiruPartitionEventHandler partitionEventHandler = new MiruPartitionEventHandler(clusterRegistry);
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        MiruNoOpActivityWAL activityWALReader = new MiruNoOpActivityWAL();
        HttpClientFactory httpClientFactory = new HttpClientFactoryProvider()
            .createHttpClientFactory(Collections.<HttpClientConfiguration>emptyList());
        MiruHostedPartitionComparison partitionComparison = new MiruHostedPartitionComparison(100, 95);
        MiruExpectedTenants expectedTenants = new MiruClusterExpectedTenants(config, miruHost, scheduledExecutorService, clusterRegistry, partitionComparison,
            factory, activityWALReader, partitionEventHandler, httpClientFactory, new ObjectMapper());
        expectedTenants.expect(Collections.singletonList(tenant1));

        MiruActivityWALWriter activityWALWriter = new MiruNoOpActivityWAL();
        MiruActivityLookupTable activityLookupTable = new MiruNoOpActivityLookupTable();
        MiruPartitionDirector miruPartitionDirector = new MiruPartitionDirector(miruHost, clusterRegistry, expectedTenants);
        miruPartitionDirector.ensureServerPartitions();

        MiruHostedPartition miruHostedPartition = miruPartitionDirector.getQueryablePartition(coord).get();
        long t = System.currentTimeMillis();
        while (miruHostedPartition.getState() != MiruPartitionState.online) {
            Thread.sleep(10);
            if (System.currentTimeMillis() - t > TimeUnit.SECONDS.toMillis(10)) {
                Assert.fail("Partition failed to come online");
            }
        }

        this.service = new MiruService(config, miruHost, Executors.newCachedThreadPool(), Executors.newScheduledThreadPool(2), Executors.newCachedThreadPool(),
            miruPartitionDirector, partitionComparison, activityWALWriter, activityLookupTable);
    }

    @Test(groups = "slow", enabled = false, description = "This test is disabled because it is very slow, enable it when you want to run it (duh)")
    public void basicTest() throws Exception {
        DecimalFormat formatter = new DecimalFormat("###,###,###");
        capacity = 1_000_000;

        MiruServiceConfig config = mock(MiruServiceConfig.class);
        when(config.getBitsetBufferSize()).thenReturn(8192);
        when(config.getHeartbeatIntervalInMillis()).thenReturn(60000L);
        when(config.getEnsurePartitionsIntervalInMillis()).thenReturn(60000L);
        when(config.getPartitionBootstrapIntervalInMillis()).thenReturn(60000L);
        when(config.getPartitionRunnableIntervalInMillis()).thenReturn(60000L);
        MiruReadTrackingWALReader miruReadTrackingWALReader = mock(MiruReadTrackingWALReader.class);

        MiruHost miruHost = new MiruHost("logicalName", 1234);
        MiruStreamFactory factory = new MiruStreamFactory(config, new MiruSchema(ImmutableMap.copyOf(rawSchema)), Executors.newCachedThreadPool(),
            miruReadTrackingWALReader, new MiruTempDirectoryResourceLocator(), new MiruTempDirectoryResourceLocator());
        MiruClusterRegistry clusterRegistry = new MiruInMemoryClusterRegistry();
        MiruPartitionEventHandler partitionEventHandler = new MiruPartitionEventHandler(clusterRegistry);
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        HttpClientFactory httpClientFactory = new HttpClientFactoryProvider()
            .createHttpClientFactory(Collections.<HttpClientConfiguration>emptyList());
        MiruNoOpActivityWAL activityWALReader = new MiruNoOpActivityWAL();
        MiruHostedPartitionComparison partitionComparison = new MiruHostedPartitionComparison(100, 95);
        MiruExpectedTenants expectedTenants = new MiruClusterExpectedTenants(config, miruHost, scheduledExecutorService, clusterRegistry, partitionComparison,
            factory, activityWALReader, partitionEventHandler, httpClientFactory, new ObjectMapper());

        MiruActivityWALWriter activityWALWriter = new MiruNoOpActivityWAL();
        MiruActivityLookupTable activityLookupTable = new MiruNoOpActivityLookupTable();
        MiruPartitionDirector miruPartitionDirector = new MiruPartitionDirector(miruHost, clusterRegistry, expectedTenants);

        this.service = new MiruService(config, miruHost, Executors.newCachedThreadPool(), Executors.newScheduledThreadPool(2), Executors.newCachedThreadPool(),
            miruPartitionDirector, partitionComparison, activityWALWriter, activityLookupTable);

        Random rand = new Random(1234);
        MiruStreamId streamId = new MiruStreamId(FilerIO.longBytes(1));
        List<MiruPartitionedActivity> activities = new ArrayList<>();
        long t = System.currentTimeMillis();
        int passes = 1;
        for (int p = 0; p < passes; p++) {
            activities.clear();
            for (int i = p * (capacity / passes); i < (p + 1) * (capacity / passes); i++) {
                activities.add(generateActivity(i, rand));
                if (i % 100_000 == 0) {
                    //System.out.println("Generated:" + i);
                }
            }
            long e = (System.currentTimeMillis() - t);
            System.out.println("Created " + formatter.format(capacity / passes) + " activities in " + formatter.format(e) + " millis");

            //System.out.println("Adding " + activities.size() + " activities.");
            t = System.currentTimeMillis();
            service.writeToIndex(activities);
            e = (System.currentTimeMillis() - t);
            long seconds = e / 1000;
            int indexSize = (p + 1) * (capacity / passes);
            System.out.println("\tIndexed " + formatter.format(activities.size()) + " activities in " + formatter.format(System.currentTimeMillis() - t)
                + " millis ratePerSecond:" + formatter.format(activities.size() / (seconds < 1 ? 1 : seconds)));
            System.out.println("\t\tIndexSize:" + formatter.format(indexSize) + " sizeInBytes:" + formatter.format(service.sizeInBytes()));

            for (int q = 0; q < 2; q++) {
                List<MiruFieldFilter> fieldFilters = new ArrayList<>();
                //fieldFilters.add(new MiruFieldFilter("author", ImmutableList.of(FilerIO.intBytes(rand.nextInt(1000)))));
                List<MiruTermId> following = generateDisticts(rand, 10_000, 1_000_000);
                //System.out.println("Following:"+new MiruFieldFilter("target", ImmutableList.copyOf(following)));
                fieldFilters.add(new MiruFieldFilter("target", ImmutableList.copyOf(following)));

                MiruFilter filter = new MiruFilter(MiruFilterOperation.or,
                    Optional.of(ImmutableList.copyOf(fieldFilters)),
                    Optional.<ImmutableList<MiruFilter>>absent());
                AggregateCountsQuery query = new AggregateCountsQuery(tenant1,
                    Optional.of(streamId),
                    Optional.of(new MiruTimeRange(0, capacity)),
                    Optional.of(new MiruTimeRange(0, capacity)),
                    Optional.<MiruAuthzExpression>absent(),
                    filter,
                    Optional.<MiruFilter>absent(),
                    "",
                    "container",
                    0, 51);

                long start = System.currentTimeMillis();
                AggregateCountsResult results = service.filterInboxStreamAll(query);
                long elapse = System.currentTimeMillis() - start;
                //System.out.println("Results:" + query);
                //                for (AggregateCount a : results.results) {
                //                    System.out.println(a);
                //                }
                System.out.println("\t\t\tQuery:" + (q + 1) + " latency:" + elapse
                    + " count:" + results.results.size()
                    + " all:" + formatter.format(count(results.results))
                    + " indexSizeToLatencyRatio:" + ((double) indexSize / (double) elapse));
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
    private final int[] fieldCardinality = new int[] { 10, 10_000, 1_000_000, 10_000, 1000 };
    private final int[] fieldFrequency = new int[] { 1, 1, 1, 10, 1 };

    private MiruPartitionedActivity generateActivity(int time, Random rand) {
        Map<String, MiruTermId[]> fieldsValues = Maps.newHashMap();
        for (String fieldName : rawSchema.keySet()) {
            int index = rawSchema.get(fieldName);
            int count = 1 + rand.nextInt(fieldFrequency[index]);
            List<MiruTermId> terms = generateDisticts(rand, count, fieldCardinality[index]);
            fieldsValues.put(fieldName, terms.toArray(new MiruTermId[0]));
        }
        MiruActivity activity = new MiruActivity.Builder(tenant1, time, new String[0], 0).putFieldsValues(fieldsValues).build();
        return partitionedActivityFactory.activity(1, partitionId, 1, activity);
    }

    private List<MiruTermId> generateDisticts(Random rand, int count, int cardinality) {
        Set<MiruIBA> usedTerms = Sets.newHashSet();
        List<MiruTermId> distincts = new ArrayList<>();
        while (distincts.size() < count) {
            int term = rand.nextInt(cardinality);
            byte[] termBytes = FilerIO.intBytes(term);
            if (usedTerms.add(new MiruTermId(termBytes))) {
                distincts.add(new MiruTermId(termBytes));
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
        List<MiruTermId> following = new ArrayList<>();
        following.add(new MiruTermId(FilerIO.intBytes(container1)));
        fieldFilters.add(new MiruFieldFilter("container", ImmutableList.copyOf(following)));
        MiruFilter followingFilter = new MiruFilter(MiruFilterOperation.or,
            Optional.of(ImmutableList.copyOf(fieldFilters)),
            Optional.<ImmutableList<MiruFilter>>absent());

        fieldFilters = new ArrayList<>();
        List<MiruTermId> authors = new ArrayList<>();
        authors.add(new MiruTermId(FilerIO.intBytes(author1)));
        fieldFilters.add(new MiruFieldFilter("author", ImmutableList.copyOf(authors)));
        MiruFilter authoredByFilter = new MiruFilter(MiruFilterOperation.or,
            Optional.of(ImmutableList.copyOf(fieldFilters)),
            Optional.<ImmutableList<MiruFilter>>absent());

        MiruFilter filter = new MiruFilter(MiruFilterOperation.and,
            Optional.<ImmutableList<MiruFieldFilter>>absent(),
            Optional.of(ImmutableList.of(followingFilter, authoredByFilter)));

        //aggregateQuery:
        {
            AggregateCountsQuery query = new AggregateCountsQuery(tenant1,
                Optional.of(streamId),
                Optional.of(new MiruTimeRange(0, 1000)),
                Optional.of(new MiruTimeRange(0, 1000)),
                Optional.<MiruAuthzExpression>absent(),
                filter,
                Optional.<MiruFilter>absent(),
                "", "container", 0, 10);
            AggregateCountsResult results = service.filterInboxStreamAll(query);
            for (AggregateCount a : results.results) {
                System.out.println(a);
            }
        }

        System.out.println("--------------");

        //countQuery:
        {
            DistinctCountQuery query = new DistinctCountQuery(tenant1,
                Optional.of(streamId),
                Optional.of(new MiruTimeRange(0, 1000)),
                Optional.<MiruAuthzExpression>absent(),
                filter,
                Optional.<MiruFilter>absent(),
                "container",
                50);
            DistinctCountResult count = service.countInboxStreamAll(query);
            System.out.println(count);
        }

        activities.clear();
        activities.add(buildActivity(10, verb2, container1, target1, tag1, author1));
        service.writeToIndex(activities);

        //aggregateQuery:
        {
            AggregateCountsQuery query = new AggregateCountsQuery(tenant1,
                Optional.of(streamId),
                Optional.of(new MiruTimeRange(0, 1000)),
                Optional.of(new MiruTimeRange(0, 1000)),
                Optional.<MiruAuthzExpression>absent(),
                filter,
                Optional.<MiruFilter>absent(),
                "", "container", 0, 10);
            AggregateCountsResult results = service.filterInboxStreamAll(query);
            for (AggregateCount a : results.results) {
                System.out.println(a);
            }
        }

        System.out.println("--------------");

        //countQuery:
        {
            DistinctCountQuery query = new DistinctCountQuery(tenant1,
                Optional.of(streamId),
                Optional.of(new MiruTimeRange(0, 1000)),
                Optional.<MiruAuthzExpression>absent(),
                filter,
                Optional.<MiruFilter>absent(),
                "container",
                50);
            DistinctCountResult count = service.countInboxStreamAll(query);
            System.out.println(count);
        }

    }

    private MiruPartitionedActivity buildActivity(int time, int verb, Integer container, int target, Integer tag, int author) {
        Map<String, MiruTermId[]> fieldsValues = Maps.newHashMap();
        fieldsValues.put("verb", new MiruTermId[] { new MiruTermId(FilerIO.intBytes(verb)) });
        if (container != null) {
            fieldsValues.put("container", new MiruTermId[] { new MiruTermId(FilerIO.intBytes(container)) });
        }
        fieldsValues.put("target", new MiruTermId[] { new MiruTermId(FilerIO.intBytes(target)) });
        if (tag != null) {
            fieldsValues.put("tag", new MiruTermId[] { new MiruTermId(FilerIO.intBytes(tag)) });
        }
        fieldsValues.put("author", new MiruTermId[] { new MiruTermId(FilerIO.intBytes(author)) });
        String[] authz = new String[] { "aaabbbcccddd" };
        MiruActivity activity = new MiruActivity.Builder(tenant1, time, authz, 0).putFieldsValues(fieldsValues).build();
        return partitionedActivityFactory.activity(1, partitionId, 1, activity);
    }

}
