package com.jivesoftware.os.wiki.miru.deployable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import info.bliki.wiki.dump.Siteinfo;
import info.bliki.wiki.dump.WikiArticle;
import info.bliki.wiki.dump.WikiXMLParser;
import info.bliki.wiki.filter.PlainTextConverter;
import info.bliki.wiki.model.WikiModel;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
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
import org.apache.lucene.analysis.util.CharArraySet;

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

    public Indexer index(String tenantId, String pathToWikiDumpFile) throws Exception {

        return new Indexer(String.valueOf(idProvider.nextId()), tenantId, pathToWikiDumpFile);

    }

    public class Indexer {

        public final String indexerId;
        public final String tenantId;
        public final String pathToWikiDumpFile;
        public final AtomicLong indexed = new AtomicLong();
        public final AtomicBoolean running = new AtomicBoolean(true);
        public final long startTimestampMillis = System.currentTimeMillis();
        public String message = "";


        public Indexer(String indexerId, String tenantId, String pathToWikiDumpFile) throws NoSuchAlgorithmException {
            this.indexerId = indexerId;
            this.tenantId = tenantId;
            this.pathToWikiDumpFile = pathToWikiDumpFile;

        }


        public void start() throws Exception {
            ExecutorService tokenizers = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);
            try {
                message = "starting";
                MiruTenantId miruTenantId = new MiruTenantId(tenantId.getBytes(StandardCharsets.UTF_8));

                wikiSchemaService.ensureSchema(miruTenantId, WikiSchemaConstants.SCHEMA);

                AtomicReference<List<MiruActivity>> activities = new AtomicReference<>(Lists.newArrayList());
                AtomicReference<List<KeyAndPayload<Wiki>>> pages = new AtomicReference<>(Lists.newArrayList());
                AtomicReference<List<MiruActivity>> grams = new AtomicReference<>(Lists.newArrayList());


                List<Future<Void>> futures = Lists.newArrayList();

                WikiXMLParser wxp = new WikiXMLParser(new File(pathToWikiDumpFile), (WikiArticle page, Siteinfo stnf) -> {
                    if (page.isMain()) {

                        futures.add(tokenizers.submit(new WikiTokenizer(miruTenantId, idProvider, page, stnf, activities, pages, grams)));
                        if (futures.size() > 1000) {
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

                                List<MiruActivity> batchOfActivities = activities.getAndSet(Lists.newArrayList());
                                List<KeyAndPayload<Wiki>> batchOfPages = pages.getAndSet(Lists.newArrayList());
                                List<MiruActivity> batchOfGrams = grams.getAndSet(Lists.newArrayList());

                                futures.add(tokenizers.submit(new FlushActivities(indexed, batchOfActivities)));
                                futures.add(tokenizers.submit(new FlushPages(miruTenantId, batchOfPages)));

                            } catch (Exception x) {
                                LOG.error("ouch", x);
                            }
                        }
                    }
                    if (running.get() == false) {
                        throw new RuntimeException("Indexing Canceled");
                    }
                });
                LOG.info("Begin indexing run for {} using '{}'", tenantId, pathToWikiDumpFile);
                wxp.parse();
                LOG.info("Completed indexing run for {} using '{}'", tenantId, pathToWikiDumpFile);
                if (!activities.get().isEmpty()) {

                    List<MiruActivity> batchOfActivities = activities.getAndSet(Lists.newArrayList());
                    List<KeyAndPayload<Wiki>> batchOfPages = pages.getAndSet(Lists.newArrayList());
                    List<MiruActivity> batchOfGrams = grams.getAndSet(Lists.newArrayList());

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

    private class WikiTokenizer implements Callable<Void> {
        private final WikiModel wikiModel;
        private final PlainTextConverter converter;
        private final Analyzer analyzer;
        private final TermTokenizer termTokenizer;

        private final MiruTenantId miruTenantId;
        private final OrderIdProvider idProvider;
        private final WikiArticle page;
        private final Siteinfo stnf;

        private final AtomicReference<List<MiruActivity>> activities;
        private final AtomicReference<List<KeyAndPayload<Wiki>>> pages;
        private final AtomicReference<List<MiruActivity>> grams;

        private final List<String> stopwords = Arrays.asList("the",
            "of", "to", "and", "a", "in", "is", "it", "its", "you", "that", "he", "was", "for", "on", "are", "with", "as", "I", "his", "they", "be", "at",
            "one", "than", "have", "this", "from", "or", "had", "by", "hot", "word", "but", "what", "some", "we", "can", "out", "other", "were", "all",
            "there", "when", "up", "use", "your", "how", "said", "an", "each", "she", "which", "do", "their", "time", "if", "will", "way", "about", "any",
            "many", "then", "them", "would", "like", "so", "these", "her", "long", "make", "thing", "see", "him", "two", "has", "our", "not", "doesn't",
            "per");


        private WikiTokenizer(
            MiruTenantId miruTenantId,
            OrderIdProvider idProvider,
            WikiArticle page,
            Siteinfo stnf,
            AtomicReference<List<MiruActivity>> activities,
            AtomicReference<List<KeyAndPayload<Wiki>>> pages,
            AtomicReference<List<MiruActivity>> grams) {

            this.wikiModel = new WikiModel("https://en.wikipedia.org/wiki/${image}", "https://en.wikipedia.org/wiki/${title}");
            this.converter = new PlainTextConverter();

            this.analyzer = new EnglishAnalyzer(new CharArraySet(stopwords, true));
            this.termTokenizer = new TermTokenizer();

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

            String plainBody = wikiModel.render(converter, page.getText());

            MiruActivity ma = new MiruActivity.Builder(miruTenantId, idProvider.nextId(), 0, false, new String[0])
                .putFieldValue("id", page.getId())
                .putAllFieldValues("subject", tokenize(page.getTitle(), grams.get()))
                .putAllFieldValues("body", tokenize(plainBody, grams.get()))
                .build();
            activities.get().add(ma);

            Wiki wiki = new Wiki(page.getId(), page.getTitle(), page.getText());
            pages.get().add(new KeyAndPayload<>(page.getId(), wiki));

            return null;
        }

        private Set<String> tokenize(String plainText, List<MiruActivity> grams) {
            if (plainText == null) {
                return Collections.emptySet();
            }


            List<String> tokens = termTokenizer.tokenize(analyzer, plainText);
            int l = tokens.size();

            /*int maxGram = 5;
            for (int i = 0; i < Math.min(1, l - maxGram); i++) {
                for (int j = 1; i + j < l && j < maxGram; j++) {
                    List<String> parts = new ArrayList<>(tokens.subList(i, i + j));
                    Collections.sort(parts);


                    MiruActivity gram = new MiruActivity.Builder(miruTenantId, idProvider.nextId(), 0, false, new String[0])
                    .putFieldValue("id", page.getId())
                    .putAllFieldValues("gram", gram)
                    .putAllFieldValues("body", tokenize(plainBody, grams))
                    .build();

                    grams.add(gram);
                }
            }*/

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
                    LOG.info("Flushed {} paget to Amza in {} millis", pages.size(), (System.currentTimeMillis() - start));
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
        public final String id;
        public final String subject;
        public final String body;

        @JsonCreator
        public Wiki(@JsonProperty("id") String id,
            @JsonProperty("subject") String subject,
            @JsonProperty("body") String body) {
            this.id = id;
            this.subject = subject;
            this.body = body;
        }
    }


}
