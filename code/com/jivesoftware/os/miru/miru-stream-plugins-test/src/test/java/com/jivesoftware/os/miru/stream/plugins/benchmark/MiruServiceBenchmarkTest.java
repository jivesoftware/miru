package com.jivesoftware.os.miru.stream.plugins.benchmark;

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
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.MiruProvider;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.test.MiruPluginTestBootstrap;
import com.jivesoftware.os.miru.service.MiruService;
import com.jivesoftware.os.miru.service.bitmap.MiruBitmapsRoaring;
import com.jivesoftware.os.miru.stream.plugins.benchmark.caliper.BenchmarkSpec;
import com.jivesoftware.os.miru.stream.plugins.benchmark.caliper.InstrumentSpec;
import com.jivesoftware.os.miru.stream.plugins.benchmark.caliper.Measurement;
import com.jivesoftware.os.miru.stream.plugins.benchmark.caliper.Run;
import com.jivesoftware.os.miru.stream.plugins.benchmark.caliper.Scenario;
import com.jivesoftware.os.miru.stream.plugins.benchmark.caliper.Trial;
import com.jivesoftware.os.miru.stream.plugins.benchmark.caliper.Value;
import com.jivesoftware.os.miru.stream.plugins.filter.AggregateCounts;
import com.jivesoftware.os.miru.stream.plugins.filter.AggregateCountsInjectable;
import com.jivesoftware.os.miru.stream.plugins.filter.AggregateCountsQuery;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
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

import static com.jivesoftware.os.miru.api.activity.schema.DefaultMiruSchemaDefinition.FIELDS;

public class MiruServiceBenchmarkTest {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private static final ObjectMapper mapper = new ObjectMapper();

    private List<Integer> distinctQueries = ImmutableList.of(
        10,
        100,
        500
    );

    private List<MiruRequest<AggregateCountsQuery>> inboxAggregateCountsQueries;

    private MiruService miruService;
    private AggregateCountsInjectable injectable;
    private OrderIdProvider orderIdProvider;

    @BeforeTest
    public void setup() {
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        mapper.setVisibilityChecker(mapper.getSerializationConfig().getDefaultVisibilityChecker()
            .withFieldVisibility(JsonAutoDetect.Visibility.ANY));
    }

    @Test (groups = "slow", enabled = false, description = "This test is disabled because it is very slow, enable it when you want to run it (duh)")
    public void benchmarkTestSmallCustomer() throws Exception {
        UUID runUUID = UUID.randomUUID();
        setupAndRunBenchmark(runUUID, "benchmarkTestSmallCustomer", MiruCustomerSize.SMALL_CUSTOMER);
    }

    @Test (groups = "slow", enabled = false, description = "This test is disabled because it is very slow, enable it when you want to run it (duh)")
    public void benchmarkTestMediumCustomer() throws Exception {
        UUID runUUID = UUID.randomUUID();
        setupAndRunBenchmark(runUUID, "benchmarkTestMediumCustomer", MiruCustomerSize.MEDIUM_CUSTOMER);
    }

    @Test (groups = "slow", enabled = false, description = "This test is disabled because it is very slow, enable it when you want to run it (duh)")
    public void benchmarkTestLargeCustomer() throws Exception {
        UUID runUUID = UUID.randomUUID();
        setupAndRunBenchmark(runUUID, "benchmarkTestLargeCustomer", MiruCustomerSize.LARGE_CUSTOMER);
    }

    @Test (groups = "slow", enabled = false, description = "This test is disabled because it is very slow, enable it when you want to run it (duh)")
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
        MiruHost miruHost = new MiruHost("logicalName", 1_234);
        MiruTenantId tenantId = new MiruTenantId("benchmark".getBytes());
        MiruPartitionId partitionId = MiruPartitionId.of(0);
        MiruSchema schema = new MiruSchema(FIELDS);

        MiruProvider<MiruService> miruProvider = new MiruPluginTestBootstrap().bootstrap(tenantId, partitionId, miruHost, schema, MiruBackingStorage.memory,
            new MiruBitmapsRoaring(), Collections.<MiruPartitionedActivity>emptyList());
        this.miruService = miruProvider.getMiru(tenantId);
        this.injectable = new AggregateCountsInjectable(miruProvider, new AggregateCounts(miruProvider));

        this.orderIdProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(311));
        log.debug(currentProfile + " = Initialized MiruService");

        // Generate all of the activities and index them
        log.debug(currentProfile + " = Generating activities");
        Random random = new Random(System.currentTimeMillis());
        long maxOrderId = customerSize.generateAndIndexActivities(miruService, orderIdProvider, random, tenantId, fieldCardinality);
        log.debug(currentProfile + " = Generated activities");

        // Generate all of the inbox queries that will run for this iteration
        log.debug(currentProfile + " = Generating inbox queries");
        this.inboxAggregateCountsQueries = customerSize.generateInboxAggregateCountsQueries(random, tenantId, numDistinctQueries, fieldCardinality,
            followables, maxOrderId);
        log.debug(currentProfile + " = Generated inbox queries");
    }

    private BenchmarkResult runBenchmark() throws Exception {
        int numQueriesToRun = inboxAggregateCountsQueries.size();

        long start = System.nanoTime();
        for (int j = 0; j < numQueriesToRun; j++) {
            if ((j + 1) % 10 == 0) {
                log.info("Running query " + (j + 1) + " of " + numQueriesToRun);
            }
            injectable.filterInboxStreamAll(inboxAggregateCountsQueries.get(j));
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
            .className(MiruServiceBenchmarkTest.class.getCanonicalName())
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
            .setSocketTimeoutInMillis(10_000)
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
            return "BenchmarkProfile{"
                + "customerSize=" + customerSize
                + ", fieldCardinality=" + fieldCardinality
                + ", followables=" + followables
                + ", numDistinctQueries=" + numDistinctQueries
                + '}';
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
            return followables == that.followables;
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

    /**
     * Taken from Google Caliper to help force a GC between runs
     */
    @SuppressWarnings ("ResultOfObjectAllocationIgnored")
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
