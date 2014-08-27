/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jivesoftware.os.miru.service;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.jivesoftware.os.jive.utils.http.client.HttpClientConfiguration;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactory;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactoryProvider;
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
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
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
import com.jivesoftware.os.miru.service.bitmap.MiruBitmapsEWAH;
import com.jivesoftware.os.miru.service.stream.locator.MiruResourceLocatorProvider;
import com.jivesoftware.os.miru.wal.MiruWALInitializer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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

    MiruSchema miruSchema = new MiruSchema(
            new MiruFieldDefinition(0, "user", false, ImmutableList.of("doc"), ImmutableList.<String>of()),
            new MiruFieldDefinition(1, "doc", false, ImmutableList.of("user"), ImmutableList.of("user")));

    MiruTenantId tenant1 = new MiruTenantId("tenant1".getBytes());

    MiruPartitionedActivityFactory partitionedActivityFactory = new MiruPartitionedActivityFactory();
    MiruService service;
    MiruPartitionId partitionId = MiruPartitionId.of(1);

    @BeforeMethod
    public void setUpMethod() throws Exception {

        MiruBackingStorage desiredStorage = MiruBackingStorage.hybrid;

        MiruServiceConfig config = BindInterfaceToConfiguration.bindDefault(MiruServiceConfig.class);
        config.setDefaultStorage(desiredStorage.name());
        config.setDefaultFailAfterNMillis(TimeUnit.HOURS.toMillis(1));

        MiruHost miruHost = new MiruHost("logicalName", 1234);
        HttpClientFactory httpClientFactory = new HttpClientFactoryProvider()
                .createHttpClientFactory(Collections.<HttpClientConfiguration>emptyList());

        SetOfSortedMapsImplInitializer<RuntimeException> setOfSortedMapsImplInitializer = new InMemorySetOfSortedMapsImplInitializer();
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
        clusterRegistry.refreshTopology(new MiruPartitionCoord(tenant1, partitionId, miruHost), new MiruPartitionCoordMetrics(0, 0),
                System.currentTimeMillis());

        MiruWALInitializer.MiruWAL wal = new MiruWALInitializer().initialize("test", setOfSortedMapsImplInitializer);

        MiruLifecyle<MiruResourceLocatorProvider> miruResourceLocatorProviderLifecyle = new MiruTempResourceLocatorProviderInitializer().initialize();
        miruResourceLocatorProviderLifecyle.start();
        MiruLifecyle<MiruService> miruServiceLifecyle = new MiruServiceInitializer<EWAHCompressedBitmap>().initialize(config,
                registryStore,
                clusterRegistry,
                miruHost,
                miruSchema,
                wal,
                httpClientFactory,
                miruResourceLocatorProviderLifecyle.getService(),
                //new MiruBitmapsRoaring());
                new MiruBitmapsEWAH(config.getBitsetBufferSize()));

        miruServiceLifecyle.start();
        MiruService miruService = miruServiceLifecyle.getService();

        long t = System.currentTimeMillis();
        while (!miruService.checkInfo(tenant1, partitionId, new MiruPartitionCoordInfo(MiruPartitionState.online, desiredStorage))) {
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

        AtomicInteger time = new AtomicInteger();
        Random rand = new Random(1234);
        int numqueries = 2;
        int numberOfUsers = 2;
        int numberOfDocument = 500_000;
        int numberOfViewsPerUser = 2;
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
                long activityTime = time.incrementAndGet();
                service.writeToIndex(Collections.singletonList(viewActivity(activityTime, user, String.valueOf(docId))));
                if (++count % 10_000 == 0) {
                    System.out.println("Finished " + count + " in " + (System.currentTimeMillis() - start) + " ms");
                }
            }
        }

        System.out.println("Built and indexed " + count + " in " + (System.currentTimeMillis() - start) + "millis");

        service.writeToIndex(Collections.singletonList(viewActivity(time.incrementAndGet(), "bob0", "1")));
        service.writeToIndex(Collections.singletonList(viewActivity(time.incrementAndGet(), "bob0", "2")));
        service.writeToIndex(Collections.singletonList(viewActivity(time.incrementAndGet(), "bob0", "3")));
        service.writeToIndex(Collections.singletonList(viewActivity(time.incrementAndGet(), "bob0", "4")));
        service.writeToIndex(Collections.singletonList(viewActivity(time.incrementAndGet(), "bob0", "9")));

        service.writeToIndex(Collections.singletonList(viewActivity(time.incrementAndGet(), "frank", "1")));
        service.writeToIndex(Collections.singletonList(viewActivity(time.incrementAndGet(), "frank", "2")));
        service.writeToIndex(Collections.singletonList(viewActivity(time.incrementAndGet(), "frank", "3")));
        service.writeToIndex(Collections.singletonList(viewActivity(time.incrementAndGet(), "frank", "4")));
        service.writeToIndex(Collections.singletonList(viewActivity(time.incrementAndGet(), "frank", "10")));

        service.writeToIndex(Collections.singletonList(viewActivity(time.incrementAndGet(), "jane", "2")));
        service.writeToIndex(Collections.singletonList(viewActivity(time.incrementAndGet(), "jane", "3")));
        service.writeToIndex(Collections.singletonList(viewActivity(time.incrementAndGet(), "jane", "4")));
        service.writeToIndex(Collections.singletonList(viewActivity(time.incrementAndGet(), "jane", "11")));

        service.writeToIndex(Collections.singletonList(viewActivity(time.incrementAndGet(), "liz", "3")));
        service.writeToIndex(Collections.singletonList(viewActivity(time.incrementAndGet(), "liz", "4")));
        service.writeToIndex(Collections.singletonList(viewActivity(time.incrementAndGet(), "liz", "12")));
        service.writeToIndex(Collections.singletonList(viewActivity(time.incrementAndGet(), "liz", "12")));

        System.out.println("Running queries...");

        for (int i = 0; i < numqueries; i++) {
            String user = "bob" + rand.nextInt(numberOfUsers);
            MiruFieldFilter miruFieldFilter = new MiruFieldFilter("user", ImmutableList.of(makeComposite(user, "^", "doc")));
            MiruFilter filter = new MiruFilter(
                    MiruFilterOperation.or,
                    Optional.of(ImmutableList.of(miruFieldFilter)),
                    Optional.<ImmutableList<MiruFilter>>absent());

            long s = System.currentTimeMillis();
            RecoResult recoResult = service.collaborativeFilteringRecommendations(new RecoQuery(tenant1,
                    Optional.<MiruAuthzExpression>absent(),
                    filter,
                    "doc", "doc", "doc",
                    "user", "user", "user",
                    "doc", "doc",
                    10));

            System.out.println("recoResult:" + recoResult);
            System.out.println("Took:" + (System.currentTimeMillis() - s));
        }

    }

    private String makeComposite(String fieldValue, String separator, String fieldName) {
        return Joiner.on("").join(fieldValue, separator, fieldName);
    }

    private MiruPartitionedActivity viewActivity(long time, String user, String doc) {
        Map<String, List<String>> fieldsValues = Maps.newHashMap();
        fieldsValues.put("user", Arrays.asList(user));
        fieldsValues.put("doc", Arrays.asList(doc));

        MiruActivity activity = new MiruActivity(tenant1, time, new String[0], 0, fieldsValues, Collections.<String, List<String>>emptyMap());
        return partitionedActivityFactory.activity(1, partitionId, 1, activity);
    }


}
