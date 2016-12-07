package com.jivesoftware.os.wiki.miru.deployable;

import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.wiki.miru.deployable.region.WikiMiruPluginRegionInput;
import com.jivesoftware.os.wiki.miru.deployable.region.WikiMiruStressPluginRegion.WikiMiruStressPluginRegionInput;
import com.jivesoftware.os.wiki.miru.deployable.region.WikiQueryPluginRegion;
import java.security.NoSuchAlgorithmException;
import java.util.List;
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
        private final WikiMiruStressPluginRegionInput input;
        public final AtomicLong failed = new AtomicLong();
        public final AtomicLong totalQueryTime = new AtomicLong();
        public final AtomicLong queried = new AtomicLong();
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

            this.statistics = new DescriptiveStatistics(6_000);

        }


        public void start() throws Exception {
            try {
                message = "starting";
                while (running.get()) {
                    try {
                        String phrase = queryPhrases.get(rand.nextInt(queryPhrases.size()));
                        long start = System.currentTimeMillis();
                        String rendered = wikiMiruService.renderPlugin(pluginRegion, new WikiMiruPluginRegionInput(tenantId, phrase, "", "", input.querier));
                        long elapse = System.currentTimeMillis() - start;
                        totalQueryTime.addAndGet(elapse);

                        queried.incrementAndGet();
                        statistics.addValue(elapse);

                        float qps = 1000f / elapse;
                        if (input.qps < qps) {
                            Thread.sleep((int)Math.max(0, (1000f / input.qps) - elapse));
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
