package com.jivesoftware.os.miru.stream.plugins.benchmark;

public class MiruStreamServiceBenchmark {

    /** TODO - We can uncomment this as soon as Google caliper releases a new version so we don't have to build it manually

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    @Param({ "SMALL_CUSTOMER", "MEDIUM_CUSTOMER", "LARGE_CUSTOMER" })
    private MiruCustomerSize customerSize;

    @Param({ "LOW", "MEDIUM", "HIGH" })
    private MiruFieldCardinality fieldCardinality;

    @Param({ "SMALL", "MEDIUM", "LARGE" })
    private MiruFollowables followables;

    @Param({ "10", "100", "500" })
    private int distinctQueries;

    private MiruService miruService;
    private OrderIdProvider orderIdProvider;

    private List<AggregateCountsQuery> inboxAggregateCountsQueries;

    @BeforeExperiment void setUp() throws Exception {
        String currentProfile = customerSize.name() + ":" + fieldCardinality.name() + ":" + followables.name() + ":" + distinctQueries;

        // Initialize the MiruService
        log.info(currentProfile + " = Initializing MiruService");
        MiruStreamFactory factory = new MiruStreamFactory(new MiruSchema(FIELDS), Executors.newCachedThreadPool(), customerSize.getCapacity(), 1024);
        this.miruService = new MiruService(new MiruTenantPartitioner(factory), 1024);
        this.orderIdProvider = new OrderIdProviderImpl(311);
        log.info(currentProfile + " = Initialized MiruService");

        // Generate all of the activities and index them
        log.info(currentProfile + " = Generating activities");
        Random random = new Random(System.currentTimeMillis());
        TenantId tenantId = new TenantId("benchmark");
        long maxOrderId = customerSize.generateAndIndexActivities(miruService, orderIdProvider, random, tenantId, fieldCardinality);
        log.info(currentProfile + " = Generated activities");

        // Generate all of the inbox queries that will run for this iteration
        log.info(currentProfile + " = Generating inbox queries");
        this.inboxAggregateCountsQueries = customerSize.generateInboxAggregateCountsQueries(
            random, tenantId, distinctQueries, fieldCardinality, followables, maxOrderId);
        log.info(currentProfile + " = Generated inbox queries");
    }

    public static void main(String[] args) {
        CaliperMain.main(MiruStreamServiceBenchmark.class, args);
    }

    @Macrobenchmark void queryInboxAll() throws Exception {
        for (int j = 0; j < distinctQueries; j++) {
            miruService.filterInboxStreamAll(inboxAggregateCountsQueries.get(j));
        }
    }
    */
}