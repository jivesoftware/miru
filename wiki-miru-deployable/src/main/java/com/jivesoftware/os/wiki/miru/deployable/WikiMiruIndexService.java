package com.jivesoftware.os.wiki.miru.deployable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
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
import com.jivesoftware.os.wiki.miru.deployable.storage.KeyAndPayload;
import com.jivesoftware.os.wiki.miru.deployable.storage.WikiMiruGramsAmza;
import com.jivesoftware.os.wiki.miru.deployable.storage.WikiMiruPayloadsAmza;
import com.jivesoftware.os.wiki.miru.deployable.topics.EnStopwords;
import com.jivesoftware.os.wiki.miru.deployable.topics.KeywordsExtractor;
import com.jivesoftware.os.wiki.miru.deployable.topics.KeywordsExtractor.Topic;
import info.bliki.wiki.dump.Siteinfo;
import info.bliki.wiki.dump.WikiArticle;
import info.bliki.wiki.dump.WikiXMLParser;
import info.bliki.wiki.filter.PlainTextConverter;
import info.bliki.wiki.model.WikiModel;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;

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
    private final RoundRobinStrategy roundRobinStrategy = new RoundRobinStrategy();

    public WikiMiruIndexService(OrderIdProvider idProvider,
        WikiSchemaService wikiSchemaService,
        String miruIngressEndpoint,
        ObjectMapper activityMapper,
        TenantAwareHttpClient<String> miruWriter,
        WikiMiruPayloadsAmza payloads, WikiMiruGramsAmza wikiMiruGramsAmza) {
        this.idProvider = idProvider;
        this.wikiSchemaService = wikiSchemaService;
        this.miruIngressEndpoint = miruIngressEndpoint;
        this.activityMapper = activityMapper;
        this.miruWriter = miruWriter;
        this.payloads = payloads;
        this.wikiMiruGramsAmza = wikiMiruGramsAmza;
    }

    public Indexer index(String tenantId, String pathToWikiDumpFile, int batchSize) throws Exception {

        return new Indexer(String.valueOf(idProvider.nextId()), tenantId, pathToWikiDumpFile, batchSize);

    }

    public class Indexer {

        public final String indexerId;
        public final String tenantId;
        public final String pathToWikiDumpFile;
        private final int batchSize;
        public final AtomicLong indexed = new AtomicLong();
        public final AtomicBoolean running = new AtomicBoolean(true);
        public final long startTimestampMillis = System.currentTimeMillis();
        public String message = "";


        public Indexer(String indexerId, String tenantId, String pathToWikiDumpFile, int batchSize) throws NoSuchAlgorithmException {
            this.indexerId = indexerId;
            this.tenantId = tenantId;
            this.pathToWikiDumpFile = pathToWikiDumpFile;
            this.batchSize = batchSize;

        }


        public void start() throws Exception {
            ExecutorService tokenizers = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);
            try {
                message = "starting";
                MiruTenantId miruTenantId = new MiruTenantId(tenantId.getBytes(StandardCharsets.UTF_8));

                wikiSchemaService.ensureSchema(miruTenantId, WikiSchemaConstants.SCHEMA);

                AtomicReference<List<MiruActivity>> activities = new AtomicReference<>(Collections.synchronizedList(Lists.newArrayList()));
                AtomicReference<List<KeyAndPayload<Wiki>>> pages = new AtomicReference<>(Collections.synchronizedList(Lists.newArrayList()));
                AtomicReference<List<MiruActivity>> grams = new AtomicReference<>(Collections.synchronizedList(Lists.newArrayList()));


                List<Future<Void>> futures = Lists.newArrayList();

                WikiXMLParser wxp = new WikiXMLParser(new File(pathToWikiDumpFile), (WikiArticle page, Siteinfo stnf) -> {

                    if (running.get() == false) {
                        throw new IOException("Indexing Canceled");
                    }
                    if (page.isMain()) {

                        futures.add(tokenizers.submit(new WikiTokenizer(miruTenantId, idProvider, page, stnf, activities, pages, grams)));
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

                                List<MiruActivity> batchOfActivities = activities.getAndSet(Collections.synchronizedList(Lists.newArrayList()));
                                List<KeyAndPayload<Wiki>> batchOfPages = pages.getAndSet(Collections.synchronizedList(Lists.newArrayList()));
                                List<MiruActivity> batchOfGrams = grams.getAndSet(Collections.synchronizedList(Lists.newArrayList()));

                                futures.add(tokenizers.submit(new FlushActivities(indexed, batchOfActivities)));
                                futures.add(tokenizers.submit(new FlushPages(miruTenantId, batchOfPages)));

                            } catch (Exception x) {
                                LOG.error("ouch", x);
                            }
                        }
                    }
                    if (running.get() == false) {
                        throw new IOException("Indexing Canceled");
                    }
                });
                LOG.info("Begin indexing run for {} using '{}'", tenantId, pathToWikiDumpFile);
                wxp.parse();
                LOG.info("Completed indexing run for {} using '{}'", tenantId, pathToWikiDumpFile);
                if (running.get() && !activities.get().isEmpty()) {

                    List<MiruActivity> batchOfActivities = activities.getAndSet(Collections.synchronizedList(Lists.newArrayList()));
                    List<KeyAndPayload<Wiki>> batchOfPages = pages.getAndSet(Collections.synchronizedList(Lists.newArrayList()));
                    List<MiruActivity> batchOfGrams = grams.getAndSet(Collections.synchronizedList(Lists.newArrayList()));

                    new FlushActivities(indexed, batchOfActivities).call();
                    new FlushPages(miruTenantId, batchOfPages).call();
                }
            } finally {
                message = "done";
                running.set(false);
                if (tokenizers != null) {
                    tokenizers.shutdownNow();
                }
            }
        }
    }

    private final ThreadLocal<WikiModel> wikiModelThreadLocal = ThreadLocal.withInitial(
        () -> new WikiModel("https://en.wikipedia.org/wiki/${image}", "https://en.wikipedia.org/wiki/${title}"));

    private class WikiTokenizer implements Callable<Void> {

        private final MiruTenantId miruTenantId;
        private final OrderIdProvider idProvider;
        private final WikiArticle page;
        private final Siteinfo stnf;

        private final AtomicReference<List<MiruActivity>> activities;
        private final AtomicReference<List<KeyAndPayload<Wiki>>> pages;
        private final AtomicReference<List<MiruActivity>> grams;

        private WikiTokenizer(
            MiruTenantId miruTenantId,
            OrderIdProvider idProvider,
            WikiArticle page,
            Siteinfo stnf,
            AtomicReference<List<MiruActivity>> activities,
            AtomicReference<List<KeyAndPayload<Wiki>>> pages,
            AtomicReference<List<MiruActivity>> grams) {


            this.miruTenantId = miruTenantId;
            this.idProvider = idProvider;
            this.page = page;
            this.stnf = stnf;
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

            KeywordsExtractor.Topic[] topics = KeywordsExtractor.getKeywordsList(plainBody, 10);
            StringBuilder topicsBody = new StringBuilder();
            for (Topic topic : topics) {
                if (topic.score > 1) {
                    if (topicsBody.length() != 0) {
                        topicsBody.append(", ");
                    }
                    topicsBody.append(Joiner.on(' ').join(topic.topic));
                }
            }


            MiruActivity ma = new MiruActivity.Builder(miruTenantId, idProvider.nextId(), 0, false, new String[0])
                .putFieldValue("locale", "en")
                //.publicFieldValue("timestampInMDYHMS") // TODO
                //.putFieldValue("userGuid", "") // TODO
                //.putFieldValue("folderGuid", "") // TODO
                .putFieldValue("guid", page.getId())
                .putFieldValue("verb", "import")
                .putFieldValue("type", "wiki")
                .putAllFieldValues("title", tokenize(termTokenizer, analyzer, page.getTitle(), grams.get()))
                .putAllFieldValues("body", tokenize(termTokenizer, analyzer, plainBody, grams.get()))
                //.putFieldValue("bodyGuid", "") // Not applicable
                //.putFieldValue("properties", "") // Not applicable
                //.putFieldValue("edgeGuids", "") // Not applicable
                .build();
            activities.get().add(ma);

            Wiki wiki = new Wiki(page.getTitle(), page.getText());
            pages.get().add(new KeyAndPayload<>(page.getId(), wiki));

            String slug = plainBody.trim().substring(0, Math.min(plainBody.length(), 1000));  // TODO config
            wiki = new Wiki(page.getTitle(), slug);
            pages.get().add(new KeyAndPayload<>(page.getId() + "-slug", wiki));

            if (topicsBody.length() > 0) {
                pages.get().add(new KeyAndPayload<>(page.getId() + "-topics", new Wiki("topics", topicsBody.toString())));
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
                while (true) {
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
        private final MiruTenantId miruTenantId;
        private final List<KeyAndPayload<Wiki>> pages;

        private FlushPages(MiruTenantId miruTenantId, List<KeyAndPayload<Wiki>> pages) {
            this.miruTenantId = miruTenantId;
            this.pages = pages;
        }

        @Override
        public Void call() throws Exception {

            while (true) {
                try {
                    long start = System.currentTimeMillis();
                    payloads.multiPut(miruTenantId, pages);
                    LOG.info("Flushed {} pages to Amza in {} millis", pages.size(), (System.currentTimeMillis() - start));
                    break;
                } catch (Exception x) {
                    LOG.warn("Failed to grams ", x);
                    Thread.currentThread().sleep(10_000);
                }
            }

            return null;
        }
    }


    public static class Wiki {
        public final String subject;
        public final String body;

        @JsonCreator
        public Wiki(@JsonProperty("subject") String subject,
            @JsonProperty("body") String body) {
            this.subject = subject;
            this.body = body;
        }
    }


}
