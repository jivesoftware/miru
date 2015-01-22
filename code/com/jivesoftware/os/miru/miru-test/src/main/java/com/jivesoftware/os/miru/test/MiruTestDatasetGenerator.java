package com.jivesoftware.os.miru.test;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

/**
 *
 */
public class MiruTestDatasetGenerator {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    public static void main(String... args) throws Exception {
        Options options = new Options();
        options
            .addOption("h", "help", false, "Show this help")
            .addOption("o", "output-path", true, "The path in which to output files containing the dataset (required)")
            .addOption("p", "num-partitions", true, "The number of partitions (default 1)")
            .addOption("r", "random-seed", true, "The random seed (default unique)")
            .addOption("t", "tenantId", true, "The tenantId to populate (also serves as filename prefix) (required)")
            .addOption("x", "close-partition", true, "Whether to close the latest partition (making it disk eligible) (default false)")
            .addOption(null, "bg-repair-pct", true, "The percentage of background activity that are repairs (default 0.1)")
            .addOption(null, "bg-remove-pct", true, "The percentage of background activity that are removes (default 0.01)")
            .addOption(null, "count-aggregate", true, "The desired results for aggregate counts queries (default 10)")
            .addOption(null, "count-cfilter", true, "The desired results for collaborative filtering queries (default 50)")
            .addOption(null, "count-distinct", true, "The desired results for distinct count queries (default 50)")
            .addOption(null, "count-gtrendy", true, "The desired results for global trending queries (default 50)")
            .addOption(null, "num-comments", true, "The number of new comments to emit (default 100,000)")
            .addOption(null, "num-containers", true, "The number of new containers to emit (default 0)")
            .addOption(null, "num-content", true, "The number of new content items to emit (default 10,000)")
            .addOption(null, "num-follows", true, "The number of new user follows to emit (default 100)")
            .addOption(null, "num-joins", true, "The number of new container joins to emit (default 0)")
            .addOption(null, "num-liked-comments", true, "The number of new comment likes to emit (default 10,000)")
            .addOption(null, "num-liked-content", true, "The number of new content item likes to emit (default 1,000)")
            .addOption(null, "num-stream-queries", true, "The number of queries to generate of each stream query type (default 10,000)")
            .addOption(null, "num-reco-queries", true, "The number of queries to generate of each recommender query type (default 10_000)")
            .addOption(null, "participation", true, "The weight, average, and max participants (comma separated) (default 1.1,10,40)")
            .addOption(null, "pre-comments", true, "The number of existing comments to simulate (default 10,000)")
            .addOption(null, "pre-containers", true, "The number of existing containers to simulate (default 10)")
            .addOption(null, "pre-content", true, "The number of existing content items to simulate (default 1,000)")
            .addOption(null, "pre-users", true, "The number of existing users to simulate (default 1,000)")
            .addOption(null, "public-container-pct", true, "The percentage of containers that are public (default 0.75)")
            .addOption(null, "query-containers", true, "The random upper bound of followed containers in a stream query (default 20)")
            .addOption(null, "query-users", true, "The random upper bound of followed users in a stream query (default 20)")
            .addOption(null, "writerId", true, "The writerId to populate (default 1)");

        options.getOption("o").setRequired(true);
        options.getOption("t").setRequired(true);

        CommandLineParser commandLineParser = new PosixParser();
        CommandLine commandLine;
        try {
            commandLine = commandLineParser.parse(options, args);
        } catch (Exception e) {
            printUsage(options, "Error: " + e.getMessage());
            return;
        }

        if (commandLine.hasOption("help")) {
            printUsage(options, "");
            return;
        }

        List<String> missing = Lists.newArrayList();

        String tenantId = commandLine.getOptionValue("tenantId");
        if (tenantId == null) {
            missing.add("tenantId");
        }
        String outputPath = commandLine.getOptionValue("output-path");
        if (outputPath == null) {
            missing.add("output-path");
        }

        if (!missing.isEmpty()) {
            printUsage(options, "The following options are required: " + Joiner.on(", ").join(missing));
            return;
        }

        Long randomSeed = commandLine.hasOption("r") ? Long.parseLong(commandLine.getOptionValue("random-seed")) : null;

        int numPartitions = Integer.parseInt(commandLine.getOptionValue("num-partitions", "1"));
        boolean closeFinalPartition = Boolean.parseBoolean(commandLine.getOptionValue("close-partition", "false"));

        int writerId = Integer.parseInt(commandLine.getOptionValue("writerId", "1"));

        float bgRepairPercent = Float.parseFloat(commandLine.getOptionValue("bg-repair-pct", "0.1"));
        float bgRemovePercent = Float.parseFloat(commandLine.getOptionValue("bg-remove-pct", "0.01"));

        int preUsers = Integer.parseInt(commandLine.getOptionValue("pre-users", "1000"));
        int preContainers = Integer.parseInt(commandLine.getOptionValue("pre-containers", "10"));
        int preContentItems = Integer.parseInt(commandLine.getOptionValue("pre-content", "1000"));
        int preComments = Integer.parseInt(commandLine.getOptionValue("pre-comments", "10000"));

        int numContainers = Integer.parseInt(commandLine.getOptionValue("num-containers", "10000"));
        int numContentItems = Integer.parseInt(commandLine.getOptionValue("num-content", "10000"));
        int numComments = Integer.parseInt(commandLine.getOptionValue("num-comments", "100000"));
        int numFollows = Integer.parseInt(commandLine.getOptionValue("num-follows", "100"));
        int numJoins = Integer.parseInt(commandLine.getOptionValue("num-joins", "100"));
        int numLikeContentItems = Integer.parseInt(commandLine.getOptionValue("num-liked-content", "1000"));
        int numLikeComments = Integer.parseInt(commandLine.getOptionValue("num-liked-comments", "10000"));
        int numStreamQueries = Integer.parseInt(commandLine.getOptionValue("num-stream-queries", "10000"));
        int numRecoQueries = Integer.parseInt(commandLine.getOptionValue("num-reco-queries", "10000"));
        int numResultsAggregateCounts = Integer.parseInt(commandLine.getOptionValue("count-aggregate", "10"));
        int numResultsDistinctCount = Integer.parseInt(commandLine.getOptionValue("count-distinct", "50"));
        int numResultsGlobalTrendy = Integer.parseInt(commandLine.getOptionValue("count-gtrendy", "50"));
        int numResultsCollaborativeFiltering = Integer.parseInt(commandLine.getOptionValue("count-cfilter", "50"));

        String[] participation = commandLine.getOptionValue("participation", "1.1,10,40").split(",");
        double participationWeight = Double.parseDouble(participation[0]);
        int participationAvg = Integer.parseInt(participation[1]);
        int participationMax = Integer.parseInt(participation[2]);

        float publicContainerPercent = Float.parseFloat(commandLine.getOptionValue("public-container-pct", "0.75"));

        int queryContainers = Integer.parseInt(commandLine.getOptionValue("query-containers", "20"));
        int queryUsers = Integer.parseInt(commandLine.getOptionValue("query-users", "20"));

        List<TypeAndCount> activityCounts = Lists.newArrayList(
            new TypeAndCount(MiruTestActivityType.createPlace, numContainers),
            new TypeAndCount(MiruTestActivityType.contentItem, numContentItems),
            new TypeAndCount(MiruTestActivityType.comment, numComments),
            new TypeAndCount(MiruTestActivityType.userFollow, numFollows),
            new TypeAndCount(MiruTestActivityType.joinPlace, numJoins),
            new TypeAndCount(MiruTestActivityType.likeContentItem, numLikeContentItems),
            new TypeAndCount(MiruTestActivityType.likeComment, numLikeComments));

        int totalActivities = 0;
        for (TypeAndCount activityCount : activityCounts) {
            totalActivities += activityCount.count;
        }

        Random random = randomSeed != null ? new Random(randomSeed) : new Random();
        MiruTestFeatureSupplier featureSupplier = new MiruTestCollectionBackedFeatureSupplier(
            random, publicContainerPercent, tenantId,
            preUsers, preContainers, preContentItems, preComments,
            new TwoTailedRandomNumber(0, participationAvg, participationMax, participationWeight, participationWeight, participationAvg,
                participationMax - participationAvg),
            totalActivities);
        MiruTestActivityDistributor activityDistributor = new MiruTestActivityDistributor(
            random,
            featureSupplier,
            bgRepairPercent,
            bgRemovePercent,
            activityCounts,
            totalActivities);
        MiruTestStreamQueryDistributor queryDistributor = new MiruTestStreamQueryDistributor(random, featureSupplier, numStreamQueries, queryUsers,
            queryContainers, numResultsAggregateCounts, numResultsDistinctCount);
        MiruTestRecoQueryDistributor trendyDistributor = new MiruTestRecoQueryDistributor(numRecoQueries, featureSupplier, numResultsGlobalTrendy,
            numResultsCollaborativeFiltering);

        MiruTestDatasetGenerator generator = new MiruTestDatasetGenerator(
            writerId,
            featureSupplier,
            activityDistributor,
            queryDistributor,
            trendyDistributor,
            numPartitions,
            closeFinalPartition);

        final Configuration hadoopConfiguration = new Configuration();
        hadoopConfiguration.set("fs.hdfs.impl", "org.apache.hadoop.fs.LocalFileSystem");
        hadoopConfiguration.set("dfs.block.size", String.valueOf(128 * 1_024 * 1_024));
        FileSystem fileSystem = FileSystem.get(hadoopConfiguration);
        generator.generate(
            writer(hadoopConfiguration, outputPath, tenantId + ".meta", Text.class, Text.class, fileSystem),
            writer(hadoopConfiguration, outputPath, tenantId + ".activity", LongWritable.class, Text.class, fileSystem),
            writer(hadoopConfiguration, outputPath, tenantId + ".background", LongWritable.class, Text.class, fileSystem),
            new SequenceFileRevisitor(hadoopConfiguration, outputPath, tenantId),
            writer(hadoopConfiguration, outputPath, tenantId + ".query.aggregate-custom", LongWritable.class, Text.class, fileSystem),
            writer(hadoopConfiguration, outputPath, tenantId + ".query.aggregate-inbox", LongWritable.class, Text.class, fileSystem),
            writer(hadoopConfiguration, outputPath, tenantId + ".query.distinct-custom", LongWritable.class, Text.class, fileSystem),
            writer(hadoopConfiguration, outputPath, tenantId + ".query.distinct-inbox", LongWritable.class, Text.class, fileSystem),
            writer(hadoopConfiguration, outputPath, tenantId + ".query.global-trendy", LongWritable.class, Text.class, fileSystem),
            writer(hadoopConfiguration, outputPath, tenantId + ".query.collaborative-filtering", LongWritable.class, Text.class, fileSystem));
    }

    private static SequenceFile.Writer writer(Configuration hadoopConfiguration, String dirName, String fileName, Class<?> keyClass, Class<?> valueClass,
        FileSystem fileSystem) throws IOException {
        Path path = new Path(dirName, fileName);
        fileSystem.delete(path, false);
        return SequenceFile.createWriter(
            hadoopConfiguration,
            SequenceFile.Writer.file(path),
            SequenceFile.Writer.keyClass(keyClass),
            SequenceFile.Writer.valueClass(valueClass));
    }

    private static void printUsage(Options options, String footer) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(MiruTestDatasetGenerator.class.getSimpleName(), "Generate a miru dataset", options, footer, true);
    }

    private final int writerId;
    private final MiruTestFeatureSupplier featureSupplier;
    private final MiruTestActivityDistributor activityDistributor;
    private final MiruTestStreamQueryDistributor streamQueryDistributor;
    private final MiruTestRecoQueryDistributor recoQueryDistributor;
    private final int numPartitions;
    private final boolean closeFinalPartition;

    public MiruTestDatasetGenerator(int writerId,
        MiruTestFeatureSupplier featureSupplier,
        MiruTestActivityDistributor activityDistributor,
        MiruTestStreamQueryDistributor streamQueryDistributor,
        MiruTestRecoQueryDistributor recoQueryDistributor,
        int numPartitions,
        boolean closeFinalPartition) {

        this.writerId = writerId;
        this.featureSupplier = featureSupplier;
        this.activityDistributor = activityDistributor;
        this.streamQueryDistributor = streamQueryDistributor;
        this.recoQueryDistributor = recoQueryDistributor;
        this.numPartitions = numPartitions;
        this.closeFinalPartition = closeFinalPartition;
    }

    private void generate(SequenceFile.Writer metaWriter,
        SequenceFile.Writer activityWriter,
        SequenceFile.Writer backgroundWriter,
        MiruTestActivityDistributor.Revisitor activityRevisitor,
        SequenceFile.Writer queryAggregateCustomWriter,
        SequenceFile.Writer queryAggregateInboxWriter,
        SequenceFile.Writer queryDistinctCustomWriter,
        SequenceFile.Writer queryDistinctInboxWriter,
        SequenceFile.Writer queryGlobalTrendyWriter,
        SequenceFile.Writer queryCollaborativeFilteringWriter)
        throws IOException, InterruptedException, ExecutionException {

        metaWriter.append(new Text("tenantId"), new Text(featureSupplier.miruTenantId().getBytes()));
        metaWriter.append(new Text("numPartitions"), new Text(String.valueOf(numPartitions)));
        metaWriter.append(new Text("closeFinalPartition"), new Text(String.valueOf(closeFinalPartition)));
        metaWriter.close();

        ExecutorService executor = Executors.newFixedThreadPool(2);

        AtomicInteger index = new AtomicInteger();
        activityProduceAndConsume(activityWriter, index, Optional.<MiruTestActivityDistributor.Revisitor>absent(), executor);
        activityProduceAndConsume(backgroundWriter, index, Optional.of(activityRevisitor), executor);

        queryProduceAndConsume(queryAggregateCustomWriter, executor, streamQueryDistributor.getNumQueries(), new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                return streamQueryDistributor.aggregateCountsQuery(false);
            }
        });
        queryProduceAndConsume(queryAggregateInboxWriter, executor, streamQueryDistributor.getNumQueries(), new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                return streamQueryDistributor.aggregateCountsQuery(true);
            }
        });
        queryProduceAndConsume(queryDistinctCustomWriter, executor, streamQueryDistributor.getNumQueries(), new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                return streamQueryDistributor.distinctCountQuery(false);
            }
        });
        queryProduceAndConsume(queryDistinctInboxWriter, executor, streamQueryDistributor.getNumQueries(), new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                return streamQueryDistributor.distinctCountQuery(true);
            }
        });
        queryProduceAndConsume(queryGlobalTrendyWriter, executor, recoQueryDistributor.getNumQueries(), new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                return recoQueryDistributor.globalTrending();
            }
        });
        queryProduceAndConsume(queryCollaborativeFilteringWriter, executor, recoQueryDistributor.getNumQueries(), new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                return recoQueryDistributor.collaborativeFiltering();
            }
        });

        executor.shutdown();
        while (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
            log.info("Waiting for executor to finish...");
        }
        activityWriter.close();
        backgroundWriter.close();
    }

    private void activityProduceAndConsume(SequenceFile.Writer activityWriter, AtomicInteger index, Optional<MiruTestActivityDistributor.Revisitor> revisitor,
        ExecutorService executor) throws ExecutionException, InterruptedException, IOException {

        BlockingQueue<MiruPartitionedActivity> queue = Queues.newArrayBlockingQueue(1_000);
        AtomicBoolean done = new AtomicBoolean(false);

        Future<?> producer = executor.submit(new ActivityProducerRunnable(activityDistributor, featureSupplier, numPartitions, closeFinalPartition, writerId,
            queue, index, revisitor, done));
        Future<?> consumer = executor.submit(new ActivityConsumerRunnable(activityWriter, queue, done));

        log.info("Waiting for activity producer to finish...");
        producer.get();

        log.info("Waiting for activity consumer to finish...");
        consumer.get();

        activityWriter.close();
        if (revisitor.isPresent()) {
            revisitor.get().close();
        }
    }

    private void queryProduceAndConsume(SequenceFile.Writer queryWriter, ExecutorService executor, int numQueries, Callable<Object> callable)
        throws ExecutionException, InterruptedException, IOException {

        BlockingQueue<Object> queue = Queues.newArrayBlockingQueue(1_000);
        AtomicBoolean done = new AtomicBoolean(false);

        Future<?> producer = executor.submit(new QueryProducerRunnable(numQueries, queue, done, callable));
        Future<?> consumer = executor.submit(new QueryConsumerRunnable(queryWriter, queue, done));

        log.info("Waiting for query producer to finish...");
        producer.get();

        log.info("Waiting for query consumer to finish...");
        consumer.get();

        queryWriter.close();
    }

}
