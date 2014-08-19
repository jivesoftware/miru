/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jivesoftware.os.miru.service.benchmark;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.jivesoftware.os.jive.utils.http.client.HttpClientConfiguration;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactory;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactoryProvider;
import com.jivesoftware.os.jive.utils.io.FilerIO;
import com.jivesoftware.os.jive.utils.row.column.value.store.inmemory.InMemorySetOfSortedMapsImplInitializer;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruLifecyle;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivityFactory;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.query.AggregateCountsQuery;
import com.jivesoftware.os.miru.api.query.MiruTimeRange;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFieldFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilterOperation;
import com.jivesoftware.os.miru.api.query.result.AggregateCountsResult;
import com.jivesoftware.os.miru.api.query.result.AggregateCountsResult.AggregateCount;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.cluster.MiruRegistryStore;
import com.jivesoftware.os.miru.cluster.MiruRegistryStoreInitializer;
import com.jivesoftware.os.miru.cluster.rcvs.MiruRCVSClusterRegistry;
import com.jivesoftware.os.miru.service.MiruService;
import com.jivesoftware.os.miru.service.MiruServiceConfig;
import com.jivesoftware.os.miru.service.MiruServiceInitializer;
import com.jivesoftware.os.miru.service.MiruTempResourceLocatorProviderInitializer;
import com.jivesoftware.os.miru.service.index.MiruFieldDefinition;
import com.jivesoftware.os.miru.service.schema.MiruSchema;
import com.jivesoftware.os.miru.service.stream.locator.MiruResourceLocatorProvider;
import com.jivesoftware.os.miru.wal.MiruWALInitializer;

import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author jonathan
 */
public class ProfilebleMiruService {

    private MiruSchema miruSchema;
    private MiruFieldDefinition[] fieldDefinitions;

    public static void main(String[] args) throws Exception {
        ProfilebleMiruService perfMiruStreamServive = new ProfilebleMiruService();
        perfMiruStreamServive.setUpMethod();
        perfMiruStreamServive.basicTest();
    }

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
    MiruService service;

    public void setUpMethod() throws Exception {

        MiruServiceConfig config = mock(MiruServiceConfig.class);
        when(config.getBitsetBufferSize()).thenReturn(8192);

        MiruHost miruHost = new MiruHost("logicalName", 1234);

        HttpClientFactory httpClientFactory = new HttpClientFactoryProvider()
                .createHttpClientFactory(Collections.<HttpClientConfiguration>emptyList());


        InMemorySetOfSortedMapsImplInitializer inMemorySetOfSortedMapsImplInitializer = new InMemorySetOfSortedMapsImplInitializer();
        MiruRegistryStore registryStore = new MiruRegistryStoreInitializer().initialize("test", inMemorySetOfSortedMapsImplInitializer);
        MiruClusterRegistry clusterRegistry = new MiruRCVSClusterRegistry(registryStore.getHostsRegistry(),
                registryStore.getExpectedTenantsRegistry(),
                registryStore.getExpectedTenantPartitionsRegistry(),
                registryStore.getReplicaRegistry(),
                registryStore.getTopologyRegistry(),
                registryStore.getConfigRegistry(),
                3,
                TimeUnit.HOURS.toMillis(1));

        MiruWALInitializer.MiruWAL wal = new MiruWALInitializer().initialize("test", inMemorySetOfSortedMapsImplInitializer);

        MiruLifecyle<MiruResourceLocatorProvider> miruResourceLocatorProviderLifecyle = new MiruTempResourceLocatorProviderInitializer().initialize();
        miruResourceLocatorProviderLifecyle.start();

        this.fieldDefinitions = new MiruFieldDefinition[]{
                new MiruFieldDefinition(0, "verb"),
                new MiruFieldDefinition(1, "container"),
                new MiruFieldDefinition(2, "target"),
                new MiruFieldDefinition(3, "tag"),
                new MiruFieldDefinition(4, "author")
        };
        this.miruSchema = new MiruSchema(fieldDefinitions);

        MiruLifecyle<MiruService> miruServiceLifecyle = new MiruServiceInitializer().initialize(config,
                registryStore,
                clusterRegistry,
                miruHost,
                miruSchema,
                wal,
                httpClientFactory,
                miruResourceLocatorProviderLifecyle.getService());

        miruServiceLifecyle.start();
        this.service = miruServiceLifecyle.getService();
    }

    public void basicTest() throws Exception {
        DecimalFormat formatter = new DecimalFormat("###,###,###");
        capacity = 10_000_000;
        MiruServiceConfig config = mock(MiruServiceConfig.class);
        when(config.getBitsetBufferSize()).thenReturn(8192);

        MiruHost miruHost = new MiruHost("logicalName", 1234);
        HttpClientFactory httpClientFactory = new HttpClientFactoryProvider()
                .createHttpClientFactory(Collections.<HttpClientConfiguration>emptyList());


        InMemorySetOfSortedMapsImplInitializer inMemorySetOfSortedMapsImplInitializer = new InMemorySetOfSortedMapsImplInitializer();
        MiruRegistryStore registryStore = new MiruRegistryStoreInitializer().initialize("test", inMemorySetOfSortedMapsImplInitializer);
        MiruClusterRegistry clusterRegistry = new MiruRCVSClusterRegistry(registryStore.getHostsRegistry(),
                registryStore.getExpectedTenantsRegistry(),
                registryStore.getExpectedTenantPartitionsRegistry(),
                registryStore.getReplicaRegistry(),
                registryStore.getTopologyRegistry(),
                registryStore.getConfigRegistry(),
                3,
                TimeUnit.HOURS.toMillis(1));

        MiruWALInitializer.MiruWAL wal = new MiruWALInitializer().initialize("test", inMemorySetOfSortedMapsImplInitializer);

        MiruLifecyle<MiruResourceLocatorProvider> miruResourceLocatorProviderLifecyle = new MiruTempResourceLocatorProviderInitializer().initialize();
        miruResourceLocatorProviderLifecyle.start();
        MiruLifecyle<MiruService> miruServiceLifecyle = new MiruServiceInitializer().initialize(config,
                registryStore,
                clusterRegistry,
                miruHost,
                miruSchema,
                wal,
                httpClientFactory,
                miruResourceLocatorProviderLifecyle.getService());

        miruServiceLifecyle.start();
        this.service = miruServiceLifecyle.getService();

        Random rand = new Random(1234);
        List<MiruPartitionedActivity> activities = new ArrayList<>();
        long t = System.currentTimeMillis();
        int passes = 50;
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

            for (int q = 0; q < 500; q++) {
                List<MiruFieldFilter> fieldFilters = new ArrayList<>();
                //fieldFilters.add(new MiruFieldFilter("author", ImmutableList.of(FilerIO.intBytes(rand.nextInt(1000)))));
                List<MiruTermId> following = generateDisticts(rand, 2000, 1_000_000);
                //System.out.println("Following:"+new MiruFieldFilter("target", ImmutableList.copyOf(following)));
                fieldFilters.add(new MiruFieldFilter("target", ImmutableList.copyOf(following)));

                MiruFilter filter = new MiruFilter(MiruFilterOperation.or,
                        Optional.of(ImmutableList.copyOf(fieldFilters)),
                        Optional.<ImmutableList<MiruFilter>>absent());
                MiruStreamId streamId = new MiruStreamId(FilerIO.longBytes(1));
                AggregateCountsQuery query = new AggregateCountsQuery(tenant1,
                        Optional.of(streamId),
                        Optional.of(new MiruTimeRange(0, capacity)),
                        Optional.of(new MiruTimeRange(0, capacity)),
                        Optional.<MiruAuthzExpression>absent(),
                        filter,
                        Optional.<MiruFilter>absent(),
                        "",
                        "container",
                        0, 50); //(q*10), (q*10)+10);

                long start = System.currentTimeMillis();
                AggregateCountsResult results = service.filterInboxStreamAll(query);
                long elapse = System.currentTimeMillis() - start;
                /*
                System.out.println("Results:" + query);
                for (AggregateCount a : results.results) {
                    System.out.println(a);
                }
                */
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
    private final int[] fieldCardinality = new int[]{10, 10_000, 1_000_000, 10_000, 10_000};
    private final int[] fieldFrequency = new int[]{1, 1, 1, 10, 1};

    private MiruPartitionedActivity generateActivity(int time, Random rand) {
        Map<String, MiruTermId[]> fieldsValues = Maps.newHashMap();
        for (MiruFieldDefinition fieldDefinition : fieldDefinitions) {
            int index = fieldDefinition.fieldId;
            int count = 1 + rand.nextInt(fieldFrequency[index]);
            List<MiruTermId> terms = generateDisticts(rand, count, fieldCardinality[index]);
            fieldsValues.put(fieldDefinition.name, terms.toArray(new MiruTermId[0]));
        }
        MiruActivity activity = new MiruActivity.Builder(tenant1, time, new String[0], 0).putFieldsValues(fieldsValues).build();
        return partitionedActivityFactory.activity(1, MiruPartitionId.of(1), 1, activity); // HACK
    }

    private List<MiruTermId> generateDisticts(Random rand, int count, int cardinality) {
        Set<MiruTermId> usedTerms = Sets.newHashSet();
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

}
