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
import com.jivesoftware.os.wiki.miru.deployable.storage.WikiMiruPayloadStorage;
import com.jivesoftware.os.wiki.miru.deployable.storage.WikiMiruPayloadStorage.KeyAndPayload;
import info.bliki.wiki.dump.Siteinfo;
import info.bliki.wiki.dump.WikiArticle;
import info.bliki.wiki.dump.WikiXMLParser;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.util.CharArraySet;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

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
    private final WikiMiruPayloadStorage payloads;
    private final RoundRobinStrategy roundRobinStrategy = new RoundRobinStrategy();

    public WikiMiruIndexService(OrderIdProvider idProvider,
        WikiSchemaService wikiSchemaService,
        String miruIngressEndpoint,
        ObjectMapper activityMapper,
        TenantAwareHttpClient<String> miruWriter,
        WikiMiruPayloadStorage payloads) {
        this.idProvider = idProvider;
        this.wikiSchemaService = wikiSchemaService;
        this.miruIngressEndpoint = miruIngressEndpoint;
        this.activityMapper = activityMapper;
        this.miruWriter = miruWriter;
        this.payloads = payloads;
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

        public Indexer(String indexerId, String tenantId, String pathToWikiDumpFile) {
            this.indexerId = indexerId;
            this.tenantId = tenantId;
            this.pathToWikiDumpFile = pathToWikiDumpFile;
        }

        public void start() throws Exception {
            try {
                message = "starting";
                MiruTenantId miruTenantId = new MiruTenantId(tenantId.getBytes(StandardCharsets.UTF_8));

                wikiSchemaService.ensureSchema(miruTenantId, WikiSchemaConstants.SCHEMA);

                List<MiruActivity> activities = Lists.newArrayList();
                List<KeyAndPayload<Wiki>> pages = Lists.newArrayList();


                WikiXMLParser wxp = new WikiXMLParser(pathToWikiDumpFile, (WikiArticle page, Siteinfo stnf) -> {
                    if (page.isMain()) {
                        LOG.info(indexed + "):" + page.getTitle());
                        MiruActivity ma = new MiruActivity.Builder(miruTenantId, idProvider.nextId(), 0, false, new String[0])
                            .putFieldValue("id", page.getId())
                            .putAllFieldValues("subject", tokenize(page.getTitle()))
                            .putAllFieldValues("body", tokenize(page.getText()))
                            .build();
                        activities.add(ma);

                        Wiki wiki = new Wiki(page.getId(), page.getTitle(), page.getText());
                        pages.add(new KeyAndPayload<>(page.getId(), wiki));

                        if (activities.size() > 1000) {
                            try {
                                LOG.info("Indexing batch of {}", activities.size());
                                record(miruTenantId, pages);
                                ingress(activities);
                                indexed.addAndGet(activities.size());
                                pages.clear();
                                activities.clear();
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
                if (!activities.isEmpty()) {
                    record(miruTenantId, pages);
                    ingress(activities);
                    indexed.addAndGet(activities.size());
                    pages.clear();
                    activities.clear();
                }
            } finally {
                message = "done";
                running.set(false);
            }
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

    private final List<String> stopwords = Arrays.asList("the",
        "of", "to", "and", "a", "in", "is", "it", "its", "you", "that", "he", "was", "for", "on", "are", "with", "as", "I", "his", "they", "be", "at",
        "one", "than", "have", "this", "from", "or", "had", "by", "hot", "word", "but", "what", "some", "we", "can", "out", "other", "were", "all",
        "there", "when", "up", "use", "your", "how", "said", "an", "each", "she", "which", "do", "their", "time", "if", "will", "way", "about", "any",
        "many", "then", "them", "would", "like", "so", "these", "her", "long", "make", "thing", "see", "him", "two", "has", "our", "not", "doesn't",
        "per");

    private Set<String> tokenize(String raw) {
        if (raw == null) {
            return Collections.emptySet();
        }

        Analyzer analyzer = new EnglishAnalyzer(new CharArraySet(stopwords, true));
        TermTokenizer termTokenizer = new TermTokenizer();
        List<String> tokens = termTokenizer.tokenize(analyzer, raw.toLowerCase());

        HashSet<String> set = Sets.newHashSet();
        for (String s : tokens) {
            if (!Strings.isNullOrEmpty(s)) {
                set.add(s);
            }
        }
        return set;
    }

    private void record(MiruTenantId miruTenantId, List<KeyAndPayload<Wiki>> timedMiruActivities) throws Exception {
        payloads.multiPut(miruTenantId, timedMiruActivities);
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
