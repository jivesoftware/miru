package com.jivesoftware.os.miru.siphon.deployable;

import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.miru.siphon.deployable.region.WikiMiruPluginRegionInput;
import com.jivesoftware.os.miru.siphon.deployable.region.WikiMiruStressPluginRegion.WikiMiruStressPluginRegionInput;
import com.jivesoftware.os.miru.siphon.deployable.region.WikiQueryPluginRegion;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;

/**
 * @author jonathan.colt
 */
public class WikiMiruStressService {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final OrderIdProvider idProvider;
    private final WikiMiruService wikiMiruService;
    private final WikiQueryPluginRegion pluginRegion;

    public WikiMiruStressService(OrderIdProvider idProvider, WikiMiruService wikiMiruService, WikiQueryPluginRegion pluginRegion) {
        this.idProvider = idProvider;
        this.wikiMiruService = wikiMiruService;
        this.pluginRegion = pluginRegion;
    }


    public Stresser stress(String tenantId, List<String> queryPhrase, WikiMiruStressPluginRegionInput input) throws Exception {
        return new Stresser(String.valueOf(idProvider.nextId()), tenantId, queryPhrase, input);

    }

    public class Stresser {

        public final Random rand;
        public final String stresserId;
        public final String tenantId;
        private final List<String> queryPhrases;
        public final WikiMiruStressPluginRegionInput input;
        public final AtomicLong failed = new AtomicLong();
        public final AtomicLong totalQueryTime = new AtomicLong();
        public final AtomicLong queried = new AtomicLong();

        public final AtomicLong totalAmzaTime = new AtomicLong();
        public final AtomicLong amzaGets = new AtomicLong();


        public final AtomicBoolean running = new AtomicBoolean(true);
        public final long startTimestampMillis = System.currentTimeMillis();
        public String message = "";
        public final DescriptiveStatistics statistics;


        public Stresser(String stresserId, String tenantId, List<String> queryPhrases, WikiMiruStressPluginRegionInput input) throws NoSuchAlgorithmException {
            this.stresserId = stresserId;
            this.tenantId = tenantId;
            this.queryPhrases = queryPhrases;
            this.input = input;

            this.rand = new Random(tenantId.hashCode());

            this.statistics = new DescriptiveStatistics(1000);

        }


        public void start() throws Exception {
            try {
                message = "starting";
                while (running.get()) {
                    try {
                        message = "running";
                        String phrase = queryPhrases.get(rand.nextInt(queryPhrases.size()));
                        long start = System.currentTimeMillis();
                        Map<String, Object> results = pluginRegion.query(
                            new WikiMiruPluginRegionInput(tenantId, phrase, "", "", input.querier, input.numberOfResult, input.wildcardExpansion));

                        if (results == null) {
                            failed.incrementAndGet();
                            Thread.sleep(10_000);
                            continue;
                        }

                        long e = 0;
                        long elapse = Long.parseLong((String) results.getOrDefault("elapse", "0"));
                        e += elapse;
                        totalQueryTime.addAndGet(elapse);
                        statistics.addValue(elapse);
                        queried.incrementAndGet();

                        elapse = Long.parseLong((String) results.getOrDefault("foldersElapse", "0"));
                        e += elapse;
                        totalQueryTime.addAndGet(elapse);
                        statistics.addValue(elapse);
                        queried.incrementAndGet();

                        elapse = Long.parseLong((String) results.getOrDefault("usersElapse", "0"));
                        e += elapse;
                        totalQueryTime.addAndGet(elapse);
                        statistics.addValue(elapse);
                        queried.incrementAndGet();


                        long amzaGetCount = Long.parseLong((String) results.getOrDefault("amzaGetCount", "0"));
                        long amzaGetElapse = Long.parseLong((String) results.getOrDefault("amzaGetElapse", "0"));

                        totalAmzaTime.addAndGet(amzaGetElapse);
                        amzaGets.addAndGet(amzaGetCount);

                        float qps = 3000f / e;
                        if (input.qps < qps) {
                            Thread.sleep((int) Math.max(0, (1000f / input.qps) - e));
                        }

                    } catch (Exception x) {
                        failed.incrementAndGet();
                        LOG.warn("Query failed", x);
                        Thread.sleep(10_000);
                    }
                }
            } finally {
                message = "done";
                running.set(false);
            }
        }
    }

}
