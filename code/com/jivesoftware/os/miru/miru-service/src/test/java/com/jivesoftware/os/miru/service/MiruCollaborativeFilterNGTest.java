/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jivesoftware.os.miru.service;

import com.google.common.base.Optional;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.primitives.Bytes;
import com.jivesoftware.os.jive.utils.http.client.HttpClientConfiguration;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactory;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactoryProvider;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.SetOfSortedMapsImplInitializer;
import com.jivesoftware.os.jive.utils.row.column.value.store.inmemory.InMemorySetOfSortedMapsImplInitializer;
import com.jivesoftware.os.miru.api.*;
import com.jivesoftware.os.miru.api.activity.*;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
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
import com.jivesoftware.os.miru.service.bitmap.MiruBitmapsEWAH;
import com.jivesoftware.os.miru.service.bitmap.MiruBitmapsRoaring;
import com.jivesoftware.os.miru.service.stream.locator.MiruResourceLocatorProvider;
import com.jivesoftware.os.miru.wal.MiruWALInitializer;
import org.merlin.config.BindInterfaceToConfiguration;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


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

        MiruBackingStorage disiredStorage = MiruBackingStorage.memory;

        MiruServiceConfig config = BindInterfaceToConfiguration.bindDefault(MiruServiceConfig.class);
        config.setDefaultStorage(disiredStorage.name());
        config.setDefaultFailAfterNMillis(TimeUnit.HOURS.toMillis(1));

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
        clusterRegistry.refreshTopology(new MiruPartitionCoord(tenant1, partitionId, miruHost), new MiruPartitionCoordMetrics(0, 0),
                System.currentTimeMillis());

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
                miruResourceLocatorProviderLifecyle.getService(),
                //new MiruBitmapsRoaring());
                new MiruBitmapsEWAH(config.getBitsetBufferSize()));

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


    //recoResult:RecoResult{results=[Recommendation{, distinctValue=939, rank=57.0}, Recommendation{, distinctValue=776, rank=55.0}, Recommendation{, distinctValue=713, rank=46.0}, Recommendation{, distinctValue=576, rank=45.0}, Recommendation{, distinctValue=147, rank=44.0}, Recommendation{, distinctValue=746, rank=44.0}, Recommendation{, distinctValue=74, rank=44.0}, Recommendation{, distinctValue=412, rank=43.0}, Recommendation{, distinctValue=363, rank=43.0}, Recommendation{, distinctValue=582, rank=42.0}]}
    //recoResult:RecoResult{results=[Recommendation{, distinctValue=939, rank=57.0}, Recommendation{, distinctValue=776, rank=55.0}, Recommendation{, distinctValue=713, rank=46.0}, Recommendation{, distinctValue=576, rank=45.0}, Recommendation{, distinctValue=147, rank=44.0}, Recommendation{, distinctValue=746, rank=44.0}, Recommendation{, distinctValue=74, rank=44.0}, Recommendation{, distinctValue=412, rank=43.0}, Recommendation{, distinctValue=363, rank=43.0}, Recommendation{, distinctValue=582, rank=42.0}]}


    @Test(enabled = true)
    public void basicTest() throws Exception {


        AtomicInteger time = new AtomicInteger();
        Random rand = new Random(1234);
        int numqueries = 5_000;
        int numberOfUsers = 10_000;
        int numberOfDocument = 500_000;
        int numberOfViewsPerUser = 500;
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
                service.writeToIndex(Collections.singletonList(viewActivity(time.incrementAndGet(), user, String.valueOf(docId))));
                count++;
                if (count % 10_000 == 0) {
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
            MiruFieldFilter miruFieldFilter = new MiruFieldFilter("user", ImmutableList.of(makeComposite(new MiruTermId(user.getBytes()), "^", "doc")));
            MiruFilter filter = new MiruFilter(
                    MiruFilterOperation.or,
                    Optional.of(ImmutableList.of(miruFieldFilter)),
                    Optional.<ImmutableList<MiruFilter>>absent());

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

    private MiruTermId makeComposite(MiruTermId fieldValue, String separator, String fieldName) {
        return new MiruTermId(Bytes.concat(fieldValue.getBytes(), separator.getBytes(), fieldName.getBytes()));
    }

    private MiruPartitionedActivity viewActivity(int time, String user, String doc) {
        Map<String, MiruTermId[]> fieldsValues = Maps.newHashMap();
        fieldsValues.put("user", new MiruTermId[]{new MiruTermId(user.getBytes())});
        fieldsValues.put("doc", new MiruTermId[]{new MiruTermId(doc.getBytes())});

        MiruActivity activity = new MiruActivity.Builder(miruSchema, tenant1, time, new String[0], 0).putFieldsValues(fieldsValues).build();
        return partitionedActivityFactory.activity(1, partitionId, 1, activity);
    }


}
