package com.jivesoftware.os.wiki.miru.deployable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Lists;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multiset.Entry;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeMultiset;
import com.google.common.hash.Hashing;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.query.TermTokenizer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.http.client.HttpResponse;
import com.jivesoftware.os.routing.bird.http.client.RoundRobinStrategy;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.routing.bird.shared.ClientCall;
import com.jivesoftware.os.wiki.miru.deployable.region.WikiMiruIndexPluginRegion.WikiMiruIndexPluginRegionInput;
import com.jivesoftware.os.wiki.miru.deployable.storage.KeyAndPayload;
import com.jivesoftware.os.wiki.miru.deployable.storage.WikiMiruGramsAmza;
import com.jivesoftware.os.wiki.miru.deployable.storage.WikiMiruPayloadsAmza;
import com.jivesoftware.os.wiki.miru.deployable.topics.ActivityNames;
import com.jivesoftware.os.wiki.miru.deployable.topics.EnStopwords;
import com.jivesoftware.os.wiki.miru.deployable.topics.FemaleFirstName;
import com.jivesoftware.os.wiki.miru.deployable.topics.KeywordsExtractor;
import com.jivesoftware.os.wiki.miru.deployable.topics.KeywordsExtractor.Topic;
import com.jivesoftware.os.wiki.miru.deployable.topics.LastNames;
import com.jivesoftware.os.wiki.miru.deployable.topics.MaleFirstNames;
import info.bliki.wiki.dump.Siteinfo;
import info.bliki.wiki.dump.WikiArticle;
import info.bliki.wiki.dump.WikiXMLParser;
import info.bliki.wiki.filter.PlainTextConverter;
import info.bliki.wiki.model.WikiModel;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;

/**
 * @author jonathan.colt
 */
public class WikiMiruIndexService {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    public final AtomicLong indexed = new AtomicLong();
    private final OrderIdProvider idProvider;
    private final WikiSchemaService wikiSchemaService;
    private final String miruIngressEndpoint;
    private final ObjectMapper activityMapper;
    private final TenantAwareHttpClient<String> miruWriter;
    private final WikiMiruPayloadsAmza payloads;
    private final WikiMiruGramsAmza wikiMiruGramsAmza;
    private final TransportClient esClient;
    private final RoundRobinStrategy roundRobinStrategy = new RoundRobinStrategy();

    public WikiMiruIndexService(OrderIdProvider idProvider,
        WikiSchemaService wikiSchemaService,
        String miruIngressEndpoint,
        ObjectMapper activityMapper,
        TenantAwareHttpClient<String> miruWriter,
        WikiMiruPayloadsAmza payloads,
        WikiMiruGramsAmza wikiMiruGramsAmza,
        TransportClient esClient) {

        this.idProvider = idProvider;
        this.wikiSchemaService = wikiSchemaService;
        this.miruIngressEndpoint = miruIngressEndpoint;
        this.activityMapper = activityMapper;
        this.miruWriter = miruWriter;
        this.payloads = payloads;
        this.wikiMiruGramsAmza = wikiMiruGramsAmza;
        this.esClient = esClient;
    }

    public Indexer index(WikiMiruIndexPluginRegionInput input) throws Exception {

        List<String> tenantIds = Lists.newArrayList(Splitter.on(",").trimResults().omitEmptyStrings().split(input.tenantId));
        return new Indexer(String.valueOf(idProvider.nextId()), tenantIds, input.wikiDumpFile, input.batchSize, input.miruEnabled, input.esClusterName,
            esClient);

    }

    public class Indexer {

        public final String indexerId;
        public final List<String> tenantIds;
        public final String pathToWikiDumpFile;
        private final int batchSize;
        private final boolean miruEnabled;

        private final String esClusterName;
        private final TransportClient esClient;
        private final boolean esEnabled;

        public final AtomicLong indexed = new AtomicLong();
        public final AtomicBoolean running = new AtomicBoolean(true);
        public final long startTimestampMillis = System.currentTimeMillis();
        public String message = "";


        public Indexer(String indexerId,
            List<String> tenantIds,
            String pathToWikiDumpFile,
            int batchSize,
            boolean miruEnabled,
            String esClusterName,
            TransportClient esClient) throws NoSuchAlgorithmException, UnknownHostException {
            this.indexerId = indexerId;
            this.tenantIds = tenantIds;
            this.pathToWikiDumpFile = pathToWikiDumpFile;
            this.batchSize = batchSize;
            this.miruEnabled = miruEnabled;
            this.esClusterName = esClusterName;
            this.esClient = esClient;

            this.esEnabled = esClusterName != null;
        }

        public void startTuples() throws Exception {

            Multiset<String> tuples = TreeMultiset.create();
            ExecutorService tokenizers = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);
            try {

                List<Future<Multiset<String>>> futures = Lists.newArrayList();
                WikiXMLParser wxp = new WikiXMLParser(new File(pathToWikiDumpFile), (WikiArticle page, Siteinfo stnf) -> {

                    if (running.get() == false) {
                        throw new IOException("Indexing Canceled");
                    }
                    try {
                        if (page.isMain()) {

                            futures.add(tokenizers.submit(new GramsTokenizer(page)));
                            indexed.incrementAndGet();
                            if (futures.size() > batchSize) {
                                LOG.info("Tupled {}", tuples.size());
                                long start = System.currentTimeMillis();
                                for (Future<Multiset<String>> future : futures) {
                                    try {
                                        Multiset<String> ts = future.get();

                                        for (Entry<String> t : ts.entrySet()) {
                                            tuples.add(t.getElement(), t.getCount());
                                        }
                                    } catch (Exception e) {
                                        throw new RuntimeException(e);
                                    }
                                }
                                LOG.info("Waited {} millis for {} tuplizers to complete.", (System.currentTimeMillis() - start), futures.size());
                                futures.clear();
                            }
                        }
                    } catch (Exception x) {
                        LOG.error("ouch", x);
                    }
                    if (running.get() == false) {
                        throw new IOException("Indexing Canceled");
                    }
                });
                LOG.info("Begin tuplizer run for {} using '{}'", tenantIds, pathToWikiDumpFile);
                wxp.parse();

            } finally {

                MinMaxPriorityQueue<TupleFrequence> topN = MinMaxPriorityQueue.expectedSize(100_000).maximumSize(100_000).create();
                for (Entry<String> tuple : tuples.entrySet()) {
                    topN.add(new TupleFrequence(tuple.getElement(), tuple.getCount()));
                }

                TupleFrequence[] tupleFrequences = topN.toArray(new TupleFrequence[0]);
                Arrays.sort(tupleFrequences);

                File fout = new File(new File(pathToWikiDumpFile).getParentFile(), "topNTuples.csv");
                if (fout.exists()) {
                    fout.delete();
                }
                FileOutputStream fos = new FileOutputStream(fout);

                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
                for (TupleFrequence tupleFrequence : tupleFrequences) {
                    bw.write(tupleFrequence.count + "," + tupleFrequence.tuple + "\n");
                }

                bw.close();


                message = "done";
                running.set(false);
                if (tokenizers != null) {
                    tokenizers.shutdownNow();
                }
                if (esClient != null) {
                    esClient.close();
                }
            }
        }

        class TupleFrequence implements Comparable<TupleFrequence> {
            final String tuple;
            final int count;

            TupleFrequence(String tuple, int count) {
                this.tuple = tuple;
                this.count = count;
            }


            @Override
            public int compareTo(TupleFrequence o) {

                return -Integer.compare(count, o.count);
            }
        }


        public void start() throws Exception {
            ExecutorService tokenizers = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);
            try {

                if (esClusterName != null) {
                    Settings settings = Settings.builder()
                        .put("cluster.name", "test-wiki")
                        .build();

                    if (!esClient.admin().indices().exists(new IndicesExistsRequest("wiki")).get().isExists()) {

                        CreateIndexResponse createIndexResponse = esClient.admin().indices().prepareCreate("wiki")
                            .setSettings(Settings.builder()
                                .put("index.number_of_shards", 3)
                                .put("index.number_of_replicas", 2)
                            ).get(new TimeValue(30_000));


                        PutMappingResponse response = esClient.admin().indices().preparePutMapping("wiki")
                            .setType("page")
                            .setSource(WikiSchemaConstants.esSchema())
                            .get(new TimeValue(30_000));
                    }

                }

                message = "starting";

                for (String tenantId : tenantIds) {
                    wikiSchemaService.ensureSchema(new MiruTenantId(tenantId.getBytes(StandardCharsets.UTF_8)), WikiSchemaConstants.SCHEMA);
                }

                AtomicReference<List<IndexRequest>> esIndexables = new AtomicReference<>(Collections.synchronizedList(Lists.newArrayList()));
                AtomicReference<List<MiruActivity>> activities = new AtomicReference<>(Collections.synchronizedList(Lists.newArrayList()));
                AtomicReference<List<KeyAndPayload<Content>>> pages = new AtomicReference<>(Collections.synchronizedList(Lists.newArrayList()));
                AtomicReference<List<MiruActivity>> grams = new AtomicReference<>(Collections.synchronizedList(Lists.newArrayList()));


                List<Future<Void>> futures = Lists.newArrayList();

                WikiXMLParser wxp = new WikiXMLParser(new File(pathToWikiDumpFile), (WikiArticle page, Siteinfo stnf) -> {

                    if (running.get() == false) {
                        throw new IOException("Indexing Canceled");
                    }
                    if (page.isMain()) {

                        futures.add(tokenizers.submit(new WikiTokenizer(tenantIds,
                            idProvider,
                            page,
                            stnf,
                            (esEnabled) ? esIndexables : null,
                            (miruEnabled) ? activities : null,
                            pages,
                            grams)));
                        if (futures.size() > batchSize) {
                            long start = System.currentTimeMillis();
                            for (Future<Void> future : futures) {
                                try {
                                    future.get();
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            }
                            LOG.info("Waited {} millis for {} tokenizers to complete.", (System.currentTimeMillis() - start), futures.size());
                            futures.clear();

                            try {
                                LOG.info("Indexing batch of {}", activities.get().size());

                                List<IndexRequest> batchOfEsIndexables = esIndexables.getAndSet(Collections.synchronizedList(Lists.newArrayList()));
                                List<MiruActivity> batchOfActivities = activities.getAndSet(Collections.synchronizedList(Lists.newArrayList()));
                                List<KeyAndPayload<Content>> batchOfPages = pages.getAndSet(Collections.synchronizedList(Lists.newArrayList()));
                                List<MiruActivity> batchOfGrams = grams.getAndSet(Collections.synchronizedList(Lists.newArrayList()));

                                if (esEnabled) {
                                    futures.add(tokenizers.submit(new FlushEsActivities(esClient, indexed, batchOfEsIndexables)));
                                }
                                if (miruEnabled) {
                                    futures.add(tokenizers.submit(new FlushActivities(indexed, batchOfActivities)));
                                }
                                futures.add(tokenizers.submit(new FlushPages(tenantIds, batchOfPages)));

                            } catch (Exception x) {
                                LOG.error("ouch", x);
                            }
                        }
                    }
                    if (running.get() == false) {
                        throw new IOException("Indexing Canceled");
                    }
                });
                LOG.info("Begin indexing run for {} using '{}'", tenantIds, pathToWikiDumpFile);
                wxp.parse();
                LOG.info("Completed indexing run for {} using '{}'", tenantIds, pathToWikiDumpFile);
                if (running.get() && !activities.get().isEmpty()) {

                    List<IndexRequest> batchOfEsIndexables = esIndexables.getAndSet(Collections.synchronizedList(Lists.newArrayList()));
                    List<MiruActivity> batchOfActivities = activities.getAndSet(Collections.synchronizedList(Lists.newArrayList()));
                    List<KeyAndPayload<Content>> batchOfPages = pages.getAndSet(Collections.synchronizedList(Lists.newArrayList()));
                    List<MiruActivity> batchOfGrams = grams.getAndSet(Collections.synchronizedList(Lists.newArrayList()));

                    new FlushEsActivities(esClient, indexed, batchOfEsIndexables).call();
                    new FlushActivities(indexed, batchOfActivities).call();
                    new FlushPages(tenantIds, batchOfPages).call();
                }
            } finally {
                message = "done";
                running.set(false);
                if (tokenizers != null) {
                    tokenizers.shutdownNow();
                }
                if (esClient != null) {
                    esClient.close();
                }
            }
        }
    }

    private final ThreadLocal<WikiModel> wikiModelThreadLocal = ThreadLocal.withInitial(
        () -> new WikiModel("https://en.wikipedia.org/wiki/${image}", "https://en.wikipedia.org/wiki/${title}"));


    private Set<Long> folderHashcodes = Collections.synchronizedSet(new HashSet<>());
    private Set<Long> userHashcodes = Collections.synchronizedSet(new HashSet<>());


    private class GramsTokenizer implements Callable<Multiset<String>> {

        private final WikiArticle page;

        private GramsTokenizer(WikiArticle page) {
            this.page = page;
        }


        @Override
        public Multiset<String> call() throws Exception {

            PlainTextConverter converter = new PlainTextConverter();

            Analyzer analyzer = new EnglishAnalyzer(EnStopwords.ENGLISH_STOP_WORDS_SET);
            TermTokenizer termTokenizer = new TermTokenizer();

            String plainBody = wikiModelThreadLocal.get().render(converter, page.getText());


            Multiset<String> grams = HashMultiset.create();
            List<String> tokenized = tokenize(termTokenizer, analyzer, page.getTitle().toLowerCase() + " " + plainBody.toLowerCase());
            for (String t : tokenized) {
                grams.add(t);
            }
            return grams;
        }

        private List<String> tokenize(TermTokenizer termTokenizer, Analyzer analyzer, String plainText) {
            if (plainText == null) {
                return Collections.emptyList();
            }
            List<String> grams = Lists.newArrayList();
            List<String> tokens = termTokenizer.tokenize(analyzer, plainText);
            for (int i = 0; i < tokens.size() - 2; i++) {
                grams.add(tokens.get(i) + " " + tokens.get(i + 1) + " " + tokens.get(i + 2));
            }
            return grams;
        }
    }


    private class WikiTokenizer implements Callable<Void> {

        private final List<String> tenantIds;
        private final OrderIdProvider idProvider;
        private final WikiArticle page;
        private final Siteinfo stnf;
        private final AtomicReference<List<IndexRequest>> esIndexables;
        private final AtomicReference<List<MiruActivity>> activities;
        private final AtomicReference<List<KeyAndPayload<Content>>> pages;
        private final AtomicReference<List<MiruActivity>> grams;

        private WikiTokenizer(
            List<String> tenantIds,
            OrderIdProvider idProvider,
            WikiArticle page,
            Siteinfo stnf,
            AtomicReference<List<IndexRequest>> esIndexables,
            AtomicReference<List<MiruActivity>> activities,
            AtomicReference<List<KeyAndPayload<Content>>> pages,
            AtomicReference<List<MiruActivity>> grams) {


            this.tenantIds = tenantIds;
            this.idProvider = idProvider;
            this.page = page;
            this.stnf = stnf;
            this.esIndexables = esIndexables;
            this.activities = activities;
            this.pages = pages;
            this.grams = grams;
        }


        @Override
        public Void call() throws Exception {

            PlainTextConverter converter = new PlainTextConverter();

            Analyzer analyzer = new EnglishAnalyzer(EnStopwords.ENGLISH_STOP_WORDS_SET);
            TermTokenizer termTokenizer = new TermTokenizer();

            String plainBody = wikiModelThreadLocal.get().render(converter, page.getText());

            KeywordsExtractor.Topic[] topics = KeywordsExtractor.getKeywordsList(page.getTitle() + " " + plainBody, 20, 20);
            StringBuilder topicsBody = new StringBuilder();
            String folderName = null;
            for (Topic topic : topics) {
                if (topic.score > 1) {
                    if (topicsBody.length() != 0) {
                        topicsBody.append(", ");
                    }
                    String topicName = Joiner.on(' ').join(topic.topic);
                    if (folderName == null) {
                        folderName = topicName;
                    }
                    topicsBody.append(topicName);
                }
            }

            if (folderName == null) {
                folderName = "untitled";
            }

            long folderHash = Hashing.sha256().hashString(folderName).asLong();
            String folderGuid = "folder|" + folderHash;

            Random random = new Random(new Random().nextInt(100_000));
            String firstName = (random.nextDouble() > 0.5) ? FemaleFirstName.list[random.nextInt(
                FemaleFirstName.list.length)] : MaleFirstNames.list[random.nextInt(MaleFirstNames.list.length)];
            String lastName = LastNames.list[random.nextInt(LastNames.list.length)];
            firstName = firstName.toLowerCase();
            lastName = lastName.toLowerCase();


            String userName = firstName.substring(0, 1).toUpperCase() + firstName.substring(1) + " " + lastName.substring(0,
                1).toUpperCase() + lastName.substring(1);
            long userHash = Hashing.sha256().hashString(userName).asLong();
            String userGuid = "user|" + userHash;

            if (userHashcodes.add(userHash)) {

                StringBuilder about = new StringBuilder();
                about.append("Likes:\n");
                for (int i = 0; i < 3 + random.nextInt(10); i++) {
                    about.append(ActivityNames.list[random.nextInt(ActivityNames.list.length)]).append("\n");
                }

                pages.get().add(new KeyAndPayload<>(String.valueOf(userGuid), new Content(userName, about.toString())));

                Set<String> titleTokens = tokenize(termTokenizer, analyzer, userName, grams.get());
                Set<String> bodyTokens = tokenize(termTokenizer, analyzer, about.toString(), grams.get());

                if (activities != null) {
                    for (String tenantId : tenantIds) {
                        MiruActivity ma = new MiruActivity.Builder(new MiruTenantId(tenantId.getBytes(StandardCharsets.UTF_8)), idProvider.nextId(), 0, false,
                            new String[0])
                            .putFieldValue("locale", "en")
                            //.publicFieldValue("timestampInMDYHMS") // TODO
                            //.putFieldValue("userGuid", "") // TODO
                            //.putFieldValue("folderGuid", String.valueOf(folderGuid))
                            .putFieldValue("guid", userGuid)
                            .putFieldValue("verb", "import")
                            .putFieldValue("type", "user")
                            .putAllFieldValues("title", titleTokens)
                            .putAllFieldValues("body", bodyTokens)
                            //.putFieldValue("bodyGuid", "") // Not applicable
                            //.putFieldValue("properties", "") // Not applicable
                            //.putFieldValue("edgeGuids", "") // Not applicable
                            .build();
                        activities.get().add(ma);
                    }
                }

                if (esIndexables != null) {
                    for (String tenantId : tenantIds) {
                        Map<String, Object> json = new HashMap<>();
                        json.put("tenant", tenantId);
                        json.put("locale", "en");
                        json.put("guid", userGuid);
                        json.put("verb", "import");
                        json.put("type", "user");
                        json.put("title", userName);
                        json.put("body", about.toString());


                        IndexRequest indexRequest = new IndexRequest()
                            .index("wiki")
                            .id(tenantId + "-" + userGuid)
                            .type("page")
                            .create(false)
                            .source(json);

                        esIndexables.get().add(indexRequest);
                    }
                }
            }


            if (folderHashcodes.add(folderHash)) {

                pages.get().add(new KeyAndPayload<>(String.valueOf(folderGuid), new Content(folderName, folderName)));

                for (String tenantId : tenantIds) {
                    if (activities != null) {
                        MiruActivity ma = new MiruActivity.Builder(new MiruTenantId(tenantId.getBytes(StandardCharsets.UTF_8)), idProvider.nextId(), 0, false,
                            new String[0])
                            .putFieldValue("locale", "en")
                            //.publicFieldValue("timestampInMDYHMS") // TODO
                            .putFieldValue("userGuid", userGuid)
                            //.putFieldValue("folderGuid", String.valueOf(folderGuid))
                            .putFieldValue("guid", folderGuid)
                            .putFieldValue("verb", "import")
                            .putFieldValue("type", "folder")
                            .putAllFieldValues("title", tokenize(termTokenizer, analyzer, folderName, grams.get()))
                            .putAllFieldValues("body", tokenize(termTokenizer, analyzer, folderName, grams.get()))
                            //.putFieldValue("bodyGuid", "") // Not applicable
                            //.putFieldValue("properties", "") // Not applicable
                            //.putFieldValue("edgeGuids", "") // Not applicable
                            .build();
                        activities.get().add(ma);
                    }

                    if (esIndexables != null) {

                        Map<String, Object> json = new HashMap<>();
                        json.put("tenant", tenantId);
                        json.put("locale", "en");
                        json.put("userGuid", userGuid);
                        json.put("guid", folderGuid);
                        json.put("verb", "import");
                        json.put("type", "folder");
                        json.put("title", folderName);
                        json.put("body", folderName);


                        IndexRequest indexRequest = new IndexRequest()
                            .index("wiki")
                            .id(tenantId + "-" + folderGuid)
                            .type("page")
                            .create(true)
                            .source(json);

                        esIndexables.get().add(indexRequest);
                    }
                }
            }

            String contentGuid = "content|" + page.getId();

            for (String tenantId : tenantIds) {
                if (activities != null) {
                    MiruActivity ma = new MiruActivity.Builder(new MiruTenantId(tenantId.getBytes(StandardCharsets.UTF_8)), idProvider.nextId(), 0, false,
                        new String[0])
                        .putFieldValue("locale", "en")
                        //.publicFieldValue("timestampInMDYHMS") // TODO
                        .putFieldValue("userGuid", userGuid)
                        .putFieldValue("folderGuid", folderGuid)
                        .putFieldValue("guid", contentGuid)
                        .putFieldValue("verb", "import")
                        .putFieldValue("type", "content")
                        .putAllFieldValues("title", tokenize(termTokenizer, analyzer, page.getTitle(), grams.get()))
                        .putAllFieldValues("body", tokenize(termTokenizer, analyzer, plainBody, grams.get()))
                        //.putFieldValue("bodyGuid", "") // Not applicable
                        //.putFieldValue("properties", "") // Not applicable
                        //.putFieldValue("edgeGuids", "") // Not applicable
                        .build();
                    activities.get().add(ma);
                }

                if (esIndexables != null) {
                    Map<String, Object> json = new HashMap<>();
                    json.put("tenant", tenantId);
                    json.put("locale", "en");
                    json.put("userGuid", userGuid);
                    json.put("folderGuid", folderGuid);
                    json.put("guid", contentGuid);
                    json.put("verb", "import");
                    json.put("type", "content");
                    json.put("title", page.getTitle());
                    json.put("body", plainBody);


                    IndexRequest indexRequest = new IndexRequest()
                        .index("wiki")
                        .id(tenantId + "-" + contentGuid)
                        .type("page")
                        .create(true)
                        .source(json);

                    esIndexables.get().add(indexRequest);
                }
            }

            Content content = new Content(page.getTitle(), page.getText());
            pages.get().add(new KeyAndPayload<>(contentGuid, content));

            String trimmed = plainBody.trim();
            String slug = trimmed.substring(0, Math.min(trimmed.length(), 1000));  // TODO config
            content = new Content(page.getTitle(), slug);
            pages.get().add(new KeyAndPayload<>(contentGuid + "-slug", content));

            if (topicsBody.length() > 0) {
                pages.get().add(new KeyAndPayload<>(contentGuid + "-topics", new Content("topics", topicsBody.toString())));
            }
            return null;
        }

        private Set<String> tokenize(TermTokenizer termTokenizer, Analyzer analyzer, String plainText, List<MiruActivity> grams) {
            if (plainText == null) {
                return Collections.emptySet();
            }
            List<String> tokens = termTokenizer.tokenize(analyzer, plainText);
            HashSet<String> set = Sets.newHashSet();
            for (String s : tokens) {
                if (!Strings.isNullOrEmpty(s)) {
                    set.add(s);
                }
            }
            return set;
        }
    }


    private class FlushEsActivities implements Callable<Void> {
        private final TransportClient esClient;
        private final AtomicLong indexed;
        private final List<IndexRequest> activities;

        private FlushEsActivities(TransportClient esClient, AtomicLong indexed, List<IndexRequest> activities) {
            this.esClient = esClient;
            this.indexed = indexed;
            this.activities = activities;
        }


        @Override
        public Void call() throws Exception {
            while (true) {
                try {
                    long start = System.currentTimeMillis();
                    ingress(activities);
                    LOG.info("Flushed {} activities to ES in {} millis", activities.size(), (System.currentTimeMillis() - start));
                    break;
                } catch (Exception x) {
                    LOG.warn("Failed to record", x);
                    Thread.currentThread().sleep(10_000);
                }
            }
            return null;
        }

        private void ingress(List<IndexRequest> activities) throws JsonProcessingException {
            try {

                BulkRequest bulkRequest = new BulkRequest();
                for (IndexRequest activity : activities) {
                    bulkRequest.add(activity);
                }
                while (true && !activities.isEmpty()) {
                    try {
                        ActionFuture<BulkResponse> actionFuture = esClient.bulk(bulkRequest);
                        BulkResponse bulkItemResponses = actionFuture.get(30, TimeUnit.SECONDS);
                        if (!bulkItemResponses.hasFailures()) {
                            LOG.inc("ingressed");
                            break;
                        } else {
                            try {
                                LOG.error("Failed to forward ingress to ES. message:{}. Will retry shortly....",
                                    bulkItemResponses.buildFailureMessage());
                                Thread.sleep(5000);
                            } catch (InterruptedException ex) {
                                Thread.interrupted();
                                return;
                            }
                        }
                    } catch (Exception x) {
                        try {
                            LOG.error("Failed to forward ingress to ES. Will retry shortly....", x);
                            Thread.sleep(5000);
                        } catch (InterruptedException ex) {
                            Thread.interrupted();
                            return;
                        }
                    }
                }
            } finally {
                indexed.addAndGet(activities.size());
            }
        }
    }

    private class FlushActivities implements Callable<Void> {

        private final AtomicLong indexed;
        private final List<MiruActivity> activities;

        private FlushActivities(AtomicLong indexed, List<MiruActivity> activities) {
            this.indexed = indexed;
            this.activities = activities;
        }


        @Override
        public Void call() throws Exception {
            while (true) {
                try {
                    long start = System.currentTimeMillis();
                    ingress(activities);
                    LOG.info("Flushed {} activities to Miru in {} millis", activities.size(), (System.currentTimeMillis() - start));
                    break;
                } catch (Exception x) {
                    LOG.warn("Failed to record", x);
                    Thread.currentThread().sleep(10_000);
                }
            }
            return null;
        }

        private void ingress(List<MiruActivity> activities) throws JsonProcessingException {
            try {

                String jsonActivities = activityMapper.writeValueAsString(activities);
                while (true && !activities.isEmpty()) {
                    try {
                        // TODO expose "" tenant to config?
                        HttpResponse response = miruWriter.call("", roundRobinStrategy, "ingress",
                            client -> new ClientCall.ClientResponse<>(client.postJson(miruIngressEndpoint, jsonActivities, null), true));
                        if (response.getStatusCode() < 200 || response.getStatusCode() >= 300) {
                            throw new RuntimeException("Failed to post " + activities.size() + " to " + miruIngressEndpoint);
                        }
                        LOG.inc("ingressed");
                        break;
                    } catch (Exception x) {
                        try {
                            LOG.error("Failed to forward ingress. Will retry shortly....", x);
                            Thread.sleep(5000);
                        } catch (InterruptedException ex) {
                            Thread.interrupted();
                            return;
                        }
                    }
                }
            } finally {
                indexed.addAndGet(activities.size());
            }
        }
    }

    private class FlushPages implements Callable<Void> {
        private final List<String> tenantIds;
        private final List<KeyAndPayload<Content>> pages;

        private FlushPages(List<String> tenantIds, List<KeyAndPayload<Content>> pages) {
            this.tenantIds = tenantIds;
            this.pages = pages;
        }

        @Override
        public Void call() throws Exception {

            while (true) {
                try {
                    long start = System.currentTimeMillis();
                    for (String tenantId : tenantIds) {
                        payloads.multiPut(new MiruTenantId(tenantId.getBytes(StandardCharsets.UTF_8)), pages);
                    }
                    LOG.info("Flushed {} pages to Amza in {} millis", pages.size(), (System.currentTimeMillis() - start));
                    break;
                } catch (Exception x) {
                    LOG.warn("Failed to flush pages ", x);
                    Thread.currentThread().sleep(10_000);
                }
            }

            return null;
        }
    }


    public static class Content {
        public final String title;
        public final String body;

        @JsonCreator
        public Content(@JsonProperty("title") String title,
            @JsonProperty("body") String body) {
            this.title = title;
            this.body = body;
        }
    }


}
