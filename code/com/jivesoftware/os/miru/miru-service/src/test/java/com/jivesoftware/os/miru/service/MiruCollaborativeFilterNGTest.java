/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jivesoftware.os.miru.service;

import com.google.common.base.Optional;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.jivesoftware.os.jive.utils.http.client.HttpClientConfiguration;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactory;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactoryProvider;
import com.jivesoftware.os.jive.utils.io.FilerIO;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.SetOfSortedMapsImplInitializer;
import com.jivesoftware.os.jive.utils.row.column.value.store.inmemory.InMemorySetOfSortedMapsImplInitializer;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruLifecyle;
import com.jivesoftware.os.miru.api.MiruPartition;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionCoordInfo;
import com.jivesoftware.os.miru.api.MiruPartitionCoordMetrics;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivityFactory;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.query.RecoQuery;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFieldFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilterOperation;
import com.jivesoftware.os.miru.api.query.result.RecoResult;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.cluster.MiruRegistryStore;
import com.jivesoftware.os.miru.cluster.MiruRegistryStoreInitializer;
import com.jivesoftware.os.miru.cluster.MiruReplicaSet;
import com.jivesoftware.os.miru.cluster.rcvs.MiruRCVSClusterRegistry;
import com.jivesoftware.os.miru.service.schema.MiruSchema;
import com.jivesoftware.os.miru.service.stream.locator.MiruResourceLocatorProvider;
import com.jivesoftware.os.miru.wal.MiruWALInitializer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.merlin.config.BindInterfaceToConfiguration;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * @author jonathan
 */
public class MiruCollaborativeFilterNGTest {

    MiruTenantId tenant1 = new MiruTenantId("tenant1".getBytes());

    MiruPartitionedActivityFactory partitionedActivityFactory = new MiruPartitionedActivityFactory();
    Map<String, Integer> rawSchema = new HashMap<>();
    MiruService service;
    MiruPartitionId partitionId = MiruPartitionId.of(1);

    @BeforeMethod
    public void setUpMethod() throws Exception {

        rawSchema.put("user", 0);
        rawSchema.put("doc", 1);

        Map<String, List<String>> fieldNamesBlooms = new HashMap<>();
        fieldNamesBlooms.put("doc", ImmutableList.of("user"));

        MiruSchema miruSchema = new MiruSchema(ImmutableMap.copyOf(rawSchema), fieldNamesBlooms);

        MiruBackingStorage disiredStorage = MiruBackingStorage.hybrid;

        MiruServiceConfig config = BindInterfaceToConfiguration.bindDefault(MiruServiceConfig.class);
        config.setDefaultStorage(disiredStorage.name());

        MiruHost miruHost = new MiruHost("logicalName", 1234);
        HttpClientFactory httpClientFactory = new HttpClientFactoryProvider()
                .createHttpClientFactory(Collections.<HttpClientConfiguration>emptyList());

        SetOfSortedMapsImplInitializer setOfSortedMapsImplInitializer = new InMemorySetOfSortedMapsImplInitializer();
        MiruRegistryStore registryStore = new MiruRegistryStoreInitializer().initialize("test", setOfSortedMapsImplInitializer);
        MiruClusterRegistry clusterRegistry = new MiruRCVSClusterRegistry(registryStore.getHostsRegistry(),
                registryStore.getExpectedTenantsRegistry(),
                registryStore.getExpectedTenantPartitionsRegistry(),
                registryStore.getReplicaRegistry(),
                registryStore.getTopologyRegistry(),
                registryStore.getConfigRegistry(),
                3,
                TimeUnit.HOURS.toMillis(1));

        clusterRegistry.sendHeartbeatForHost(miruHost, 0, 0);
        clusterRegistry.electToReplicaSetForTenantPartition(tenant1, partitionId,
                new MiruReplicaSet(ArrayListMultimap.<MiruPartitionState, MiruPartition>create(), new HashSet<MiruHost>(), 3));
        clusterRegistry.refreshTopology(new MiruPartitionCoord(tenant1, partitionId, miruHost), new MiruPartitionCoordMetrics(0, 0), System.currentTimeMillis());

        MiruWALInitializer.MiruWAL wal = new MiruWALInitializer().initialize("test", setOfSortedMapsImplInitializer);

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
        MiruService miruService = miruServiceLifecyle.getService();

        long t = System.currentTimeMillis();
        while (!miruService.checkInfo(tenant1, partitionId, new MiruPartitionCoordInfo(MiruPartitionState.online, disiredStorage))) {
            Thread.sleep(10);
            if (System.currentTimeMillis() - t > TimeUnit.SECONDS.toMillis(5000)) {
                Assert.fail("Partition failed to come online");
            }
        }

        this.service = miruService;
    }

    static long optimalNumOfBits(long n, double p) {
        if (p == 0) {
            p = Double.MIN_VALUE;
        }
        return (long) (-n * Math.log(p) / (Math.log(2) * Math.log(2)));
    }

    @Test(enabled = true)
    public void basicTest() throws Exception {

         // P , G, C
        // P G P
        // P C P
        // G P G
        // C P C
        // I am looking at this persons what content should I look at?
        // I am looking at this group what content should I look at?

        AtomicInteger time = new AtomicInteger();
        List<MiruPartitionedActivity> activities = new ArrayList<>();
        Random rand = new Random(1234);
        int numberOfUsers = 1;
        int numberOfDocument = 100_000;
        int numberOfViewsPerUser = 1_000;
        System.out.println("Building activities....");
        long start = System.currentTimeMillis();
        for (int i = 0; i < numberOfUsers; i++) {
            String user = "bob" + i;
            for (int d = 0; d < numberOfViewsPerUser; d++) {
                activities.add(viewActivity(time.incrementAndGet(), user, String.valueOf(rand.nextInt(numberOfDocument))));
            }
        }
        System.out.println("Built " + activities.size() + " in " + (System.currentTimeMillis() - start) + "millis");

        activities.add(viewActivity(time.incrementAndGet(), "bob0", "1"));
        activities.add(viewActivity(time.incrementAndGet(), "bob0", "2"));
        activities.add(viewActivity(time.incrementAndGet(), "bob0", "3"));
        activities.add(viewActivity(time.incrementAndGet(), "bob0", "4"));
        activities.add(viewActivity(time.incrementAndGet(), "bob0", "9"));

        activities.add(viewActivity(time.incrementAndGet(), "frank", "1"));
        activities.add(viewActivity(time.incrementAndGet(), "frank", "2"));
        activities.add(viewActivity(time.incrementAndGet(), "frank", "3"));
        activities.add(viewActivity(time.incrementAndGet(), "frank", "4"));
        activities.add(viewActivity(time.incrementAndGet(), "frank", "10"));

        activities.add(viewActivity(time.incrementAndGet(), "jane", "2"));
        activities.add(viewActivity(time.incrementAndGet(), "jane", "3"));
        activities.add(viewActivity(time.incrementAndGet(), "jane", "4"));
        activities.add(viewActivity(time.incrementAndGet(), "jane", "11"));

        activities.add(viewActivity(time.incrementAndGet(), "liz", "3"));
        activities.add(viewActivity(time.incrementAndGet(), "liz", "4"));
        activities.add(viewActivity(time.incrementAndGet(), "liz", "12"));
        activities.add(viewActivity(time.incrementAndGet(), "liz", "12"));

        System.out.println("Indexing...");
        start = System.currentTimeMillis();

//        int numPartitions = 8;
//        ExecutorService indexThread = Executors.newFixedThreadPool(numPartitions);
//        final CountDownLatch latch = new CountDownLatch(numPartitions);
//        for (final List<MiruPartitionedActivity> partition : Lists.partition(activities, activities.size() / numPartitions)) {
//
//            indexThread.submit(new Runnable() {
//
//                @Override
//                public void run() {
//                    try {
                        service.writeToIndex(activities);
//                    } catch (Exception x) {
//                        x.printStackTrace();
//                    } finally {
//                        latch.countDown();
//                    }
//                }
//            });
//        }
//        latch.await();


        System.out.println("Indexed " + activities.size() + " in " + (System.currentTimeMillis() - start) + "millis");

        for (int i = 0; i < numberOfUsers; i++) {
            String user = "bob" + i;
            MiruFieldFilter miruFieldFilter = new MiruFieldFilter("user", ImmutableList.of(new MiruTermId(user.getBytes())));
            MiruFilter filter = new MiruFilter(MiruFilterOperation.or, Optional.of(ImmutableList.of(miruFieldFilter)), Optional
                    .<ImmutableList<MiruFilter>>absent());

            start = System.currentTimeMillis();
            RecoResult recoResult = service.collaborativeFilteringRecommendations(new RecoQuery(tenant1,
                    Optional.<MiruAuthzExpression>absent(),
                    filter,
                    "doc", "doc", "doc",
                    "user", "user", "user",
                    "doc", "doc",
                    10));

            System.out.println("recoResult:" + recoResult);
            System.out.println("Took:" + (System.currentTimeMillis() - start));
        }

    }

    private MiruPartitionedActivity viewActivity(int time, String user, String doc) {
        Map<String, MiruTermId[]> fieldsValues = Maps.newHashMap();
        fieldsValues.put("user", new MiruTermId[]{new MiruTermId(user.getBytes())});
        fieldsValues.put("doc", new MiruTermId[]{new MiruTermId(doc.getBytes())});

        MiruActivity activity = new MiruActivity.Builder(tenant1, time, new String[0], 0).putFieldsValues(fieldsValues).build();
        return partitionedActivityFactory.activity(1, partitionId, 1, activity);
    }

    private MiruPartitionedActivity buildActivity(int time, int verb, Integer container, int target, Integer tag, int author) {
        Map<String, MiruTermId[]> fieldsValues = Maps.newHashMap();
        fieldsValues.put("verb", new MiruTermId[]{new MiruTermId(FilerIO.intBytes(verb))});
        if (container != null) {
            fieldsValues.put("container", new MiruTermId[]{new MiruTermId(FilerIO.intBytes(container))});
        }
        fieldsValues.put("target", new MiruTermId[]{new MiruTermId(FilerIO.intBytes(target))});
        if (tag != null) {
            fieldsValues.put("tag", new MiruTermId[]{new MiruTermId(FilerIO.intBytes(tag))});
        }
        fieldsValues.put("author", new MiruTermId[]{new MiruTermId(FilerIO.intBytes(author))});
        String[] authz = new String[]{"aaabbbcccddd"};
        MiruActivity activity = new MiruActivity.Builder(tenant1, time, authz, 0).putFieldsValues(fieldsValues).build();
        return partitionedActivityFactory.activity(1, partitionId, 1, activity);
    }

}
