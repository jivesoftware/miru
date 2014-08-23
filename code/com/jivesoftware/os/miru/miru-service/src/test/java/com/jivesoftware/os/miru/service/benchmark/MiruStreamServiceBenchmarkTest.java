package com.jivesoftware.os.miru.service.benchmark;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.jivesoftware.os.jive.utils.http.client.HttpClient;
import com.jivesoftware.os.jive.utils.http.client.HttpClientConfig;
import com.jivesoftware.os.jive.utils.http.client.HttpClientConfiguration;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactory;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactoryProvider;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.jive.utils.id.TenantId;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.jive.utils.row.column.value.store.inmemory.InMemorySetOfSortedMapsImplInitializer;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruLifecyle;
import com.jivesoftware.os.miru.api.query.AggregateCountsQuery;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.cluster.MiruRegistryStore;
import com.jivesoftware.os.miru.cluster.MiruRegistryStoreInitializer;
import com.jivesoftware.os.miru.cluster.rcvs.MiruRCVSClusterRegistry;
import com.jivesoftware.os.miru.service.MiruService;
import com.jivesoftware.os.miru.service.MiruServiceConfig;
import com.jivesoftware.os.miru.service.MiruServiceInitializer;
import com.jivesoftware.os.miru.service.MiruTempResourceLocatorProviderInitializer;
import com.jivesoftware.os.miru.service.benchmark.caliper.BenchmarkSpec;
import com.jivesoftware.os.miru.service.benchmark.caliper.InstrumentSpec;
import com.jivesoftware.os.miru.service.benchmark.caliper.Measurement;
import com.jivesoftware.os.miru.service.benchmark.caliper.Run;
import com.jivesoftware.os.miru.service.benchmark.caliper.Scenario;
import com.jivesoftware.os.miru.service.benchmark.caliper.Trial;
import com.jivesoftware.os.miru.service.benchmark.caliper.Value;
import com.jivesoftware.os.miru.service.bitmap.MiruBitmapsEWAH;
import com.jivesoftware.os.miru.api.activity.MiruSchema;
import com.jivesoftware.os.miru.service.stream.locator.MiruResourceLocatorProvider;
import com.jivesoftware.os.miru.wal.MiruWALInitializer;
import com.jivesoftware.os.miru.wal.MiruWALInitializer.MiruWAL;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static com.jivesoftware.os.miru.api.activity.schema.DefaultMiruSchemaDefinition.SCHEMA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MiruStreamServiceBenchmarkTest {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private static final ObjectMapper mapper = new ObjectMapper();

    private List<Integer> distinctQueries = ImmutableList.of(
        10,
        100,
        500
    );

    private List<AggregateCountsQuery> inboxAggregateCountsQueries;

    private MiruService miruService;
    private OrderIdProvider orderIdProvider;

    @BeforeTest
    public void setup() {
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        mapper.setVisibilityChecker(mapper.getSerializationConfig().getDefaultVisibilityChecker()
            .withFieldVisibility(JsonAutoDetect.Visibility.ANY));
    }

    @Test(groups = "slow", enabled = false, description = "This test is disabled because it is very slow, enable it when you want to run it (duh)")
    public void benchmarkTestSmallCustomer() throws Exception {
        UUID runUUID = UUID.randomUUID();
        setupAndRunBenchmark(runUUID, "benchmarkTestSmallCustomer", MiruCustomerSize.SMALL_CUSTOMER);
    }

    @Test(groups = "slow", enabled = false, description = "This test is disabled because it is very slow, enable it when you want to run it (duh)")
    public void benchmarkTestMediumCustomer() throws Exception {
        UUID runUUID = UUID.randomUUID();
        setupAndRunBenchmark(runUUID, "benchmarkTestMediumCustomer", MiruCustomerSize.MEDIUM_CUSTOMER);
    }

    @Test(groups = "slow", enabled = false, description = "This test is disabled because it is very slow, enable it when you want to run it (duh)")
    public void benchmarkTestLargeCustomer() throws Exception {
        UUID runUUID = UUID.randomUUID();
        setupAndRunBenchmark(runUUID, "benchmarkTestLargeCustomer", MiruCustomerSize.LARGE_CUSTOMER);
    }

    @Test(groups = "slow", enabled = false, description = "This test is disabled because it is very slow, enable it when you want to run it (duh)")
    public void benchmarkTestAllCustomerSizes() throws Exception {
        UUID runUUID = UUID.randomUUID();
        setupAndRunBenchmark(runUUID, "benchmarkTestAllCustomerSizes",
            MiruCustomerSize.SMALL_CUSTOMER, MiruCustomerSize.MEDIUM_CUSTOMER, MiruCustomerSize.LARGE_CUSTOMER);
    }

    private void setupAndRunBenchmark(UUID runUUID, String methodName, MiruCustomerSize... customerSizes) throws Exception {
        for (MiruCustomerSize customerSize : customerSizes) {
            for (MiruFieldCardinality fieldCardinality : MiruFieldCardinality.values()) {
                for (MiruFollowables followables : MiruFollowables.values()) {
                    for (Integer distinctQueryCount : distinctQueries) {
                        BenchmarkProfile benchmarkProfile = new BenchmarkProfile(customerSize, fieldCardinality, followables, distinctQueryCount);

                        // Do all the initial setup work for this run
                        setupBenchmark(benchmarkProfile);

                        // Run the benchmark
                        BenchmarkResult benchmarkResult = runBenchmark();

                        // Upload the results
                        uploadTrialResult(runUUID, methodName, benchmarkProfile, benchmarkResult);
                    }
                }
            }
        }

        log.info("Results uploaded to: http://microbenchmarks.appspot.com/runs/" + runUUID.toString());
    }

    private void setupBenchmark(BenchmarkProfile benchmarkProfile) throws Exception {
        MiruCustomerSize customerSize = benchmarkProfile.getCustomerSize();
        MiruFieldCardinality fieldCardinality = benchmarkProfile.getFieldCardinality();
        MiruFollowables followables = benchmarkProfile.getFollowables();
        int numDistinctQueries = benchmarkProfile.getNumDistinctQueries();

        String currentProfile = customerSize.name() + ":" + fieldCardinality.name() + ":" + followables.name() + ":" + numDistinctQueries;
        log.info("Running setup for Profile: " + currentProfile);

        forceGc();

        // Initialize the MiruService
        log.debug(currentProfile + " = Initializing MiruService");
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

        MiruWAL wal = new MiruWALInitializer().initialize("test", inMemorySetOfSortedMapsImplInitializer);

        MiruLifecyle<MiruResourceLocatorProvider> miruResourceLocatorProviderLifecyle = new MiruTempResourceLocatorProviderInitializer().initialize();
        miruResourceLocatorProviderLifecyle.start();
        MiruLifecyle<MiruService> miruServiceLifecyle = new MiruServiceInitializer().initialize(config,
                registryStore,
                clusterRegistry,
                miruHost,
                new MiruSchema(SCHEMA),
                wal,
                httpClientFactory,
                miruResourceLocatorProviderLifecyle.getService(),
                new MiruBitmapsEWAH(2)); // TODO consider feed wth data provider

        miruServiceLifecyle.start();
        this.miruService = miruServiceLifecyle.getService();


        this.orderIdProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(311));
        log.debug(currentProfile + " = Initialized MiruService");

        // Generate all of the activities and index them
        log.debug(currentProfile + " = Generating activities");
        Random random = new Random(System.currentTimeMillis());
        TenantId tenantId = new TenantId("benchmark");
        long maxOrderId = customerSize.generateAndIndexActivities(miruService, orderIdProvider, random, tenantId, fieldCardinality);
        log.debug(currentProfile + " = Generated activities");

        // Generate all of the inbox queries that will run for this iteration
        log.debug(currentProfile + " = Generating inbox queries");
        this.inboxAggregateCountsQueries = customerSize.generateInboxAggregateCountsQueries(random, tenantId, numDistinctQueries, fieldCardinality, followables,
            maxOrderId);
        log.debug(currentProfile + " = Generated inbox queries");
    }

    private BenchmarkResult runBenchmark() throws Exception {
        int numQueriesToRun = inboxAggregateCountsQueries.size();

        long start = System.nanoTime();
        for (int j = 0; j < numQueriesToRun; j++) {
            if ((j + 1) % 10 == 0) {
                log.info("Running query " + (j + 1) + " of " + numQueriesToRun);
            }
            miruService.filterInboxStreamAll(inboxAggregateCountsQueries.get(j));
        }
        long end = System.nanoTime();

        return new BenchmarkResult(end - start, numQueriesToRun);
    }

    private void uploadTrialResult(UUID runUUID, String methodName, BenchmarkProfile benchmarkProfile, BenchmarkResult benchmarkResult) {
        UUID trialId = UUID.randomUUID();

        Run run = new Run.Builder(runUUID)
            .label("")
            .startTime(new Date())
            .build();

        InstrumentSpec instrumentSpec = new InstrumentSpec.Builder()
            .className("NoClassname")
            .build();

        BenchmarkSpec benchmarkSpec = new BenchmarkSpec.Builder()
            .methodName(methodName)
            .className(MiruStreamServiceBenchmarkTest.class.getCanonicalName())
            .addParameter("customerSize", benchmarkProfile.getCustomerSize().name())
            .addParameter("fieldCardinality", benchmarkProfile.getFieldCardinality().name())
            .addParameter("followables", benchmarkProfile.getFollowables().name())
            .addParameter("numDistinctQueries", String.valueOf(benchmarkProfile.getNumDistinctQueries()))
            .build();

        Iterable<Measurement> measurements = ImmutableList.of(
            new Measurement.Builder()
                .description("runtime")
                .weight(1.0).value(Value.create(benchmarkResult.getTotalTime(), benchmarkResult.getUnit()))
                .build()
        );

        Trial trial = new Trial.Builder(trialId)
            .run(run)
            .instrumentSpec(instrumentSpec)
            .scenario(new Scenario.Builder()
                .benchmarkSpec(benchmarkSpec))
            .addAllMeasurements(measurements)
            .build();

        Collection<HttpClientConfiguration> configurations = Lists.newArrayList();
        HttpClientConfig baseConfig = HttpClientConfig.newBuilder()
            .setSocketTimeoutInMillis(10000)
            .setMaxConnections(10)
            .build();
        configurations.add(baseConfig);
        HttpClientFactory createHttpClientFactory = new HttpClientFactoryProvider().createHttpClientFactory(configurations);
        HttpClient httpClient = createHttpClientFactory.createClient("https://microbenchmarks.appspot.com/data/trials", 443);

        RequestHelper requestHelper = new RequestHelper(httpClient, mapper);
        requestHelper.executeRequest(ImmutableList.of(trial), "https://microbenchmarks.appspot.com/data/trials", String.class, "");
    }

    private class BenchmarkProfile {
        private final MiruCustomerSize customerSize;
        private final MiruFieldCardinality fieldCardinality;
        private final MiruFollowables followables;
        private final int numDistinctQueries;

        private BenchmarkProfile(MiruCustomerSize customerSize, MiruFieldCardinality fieldCardinality,
            MiruFollowables followables, int numDistinctQueries) {
            this.customerSize = customerSize;
            this.fieldCardinality = fieldCardinality;
            this.followables = followables;
            this.numDistinctQueries = numDistinctQueries;
        }

        public MiruCustomerSize getCustomerSize() {
            return customerSize;
        }

        public MiruFieldCardinality getFieldCardinality() {
            return fieldCardinality;
        }

        public MiruFollowables getFollowables() {
            return followables;
        }

        public int getNumDistinctQueries() {
            return numDistinctQueries;
        }

        @Override
        public String toString() {
            return "BenchmarkProfile{" +
                "customerSize=" + customerSize +
                ", fieldCardinality=" + fieldCardinality +
                ", followables=" + followables +
                ", numDistinctQueries=" + numDistinctQueries +
                '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            BenchmarkProfile that = (BenchmarkProfile) o;

            if (numDistinctQueries != that.numDistinctQueries) {
                return false;
            }
            if (customerSize != that.customerSize) {
                return false;
            }
            if (fieldCardinality != that.fieldCardinality) {
                return false;
            }
            if (followables != that.followables) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            int result = customerSize.hashCode();
            result = 31 * result + fieldCardinality.hashCode();
            result = 31 * result + followables.hashCode();
            result = 31 * result + numDistinctQueries;
            return result;
        }
    }

    private class BenchmarkResult {
        private final double totalTime;
        private final String unit;
        private final long numQueriesRun;

        private BenchmarkResult(double totalTime, long numQueriesRun) {
            if (totalTime < 1_000_000) {
                this.totalTime = totalTime;
                unit = "ns";
            } else {
                this.totalTime = totalTime / 1_000_000;
                unit = "ms";
            }
            this.numQueriesRun = numQueriesRun;
        }

        public double getTotalTime() {
            return totalTime;
        }

        public String getUnit() {
            return unit;
        }

        public String getAverageTimePerQuery() {
            double averageTime = totalTime / numQueriesRun;
            if (averageTime < 1_000_000) {
                return averageTime + "ns";
            }
            double averageTimeMillis = averageTime / 1_000_000;
            return averageTimeMillis + "ms";
        }
    }

    /** Taken from Google Caliper to help force a GC between runs */
    @SuppressWarnings("ResultOfObjectAllocationIgnored")
    private void forceGc() {
        System.gc();
        System.runFinalization();
        final CountDownLatch latch = new CountDownLatch(1);
        new Object() {
            @Override
            protected void finalize() {
                latch.countDown();
            }
        };
        System.gc();
        System.runFinalization();
        try {
            latch.await(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
