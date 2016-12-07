package com.jivesoftware.os.wiki.miru.deployable.region;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.ui.MiruPageRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.wiki.miru.deployable.WikiMiruStressService;
import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 */
public class WikiMiruStressPluginRegion implements MiruPageRegion<WikiMiruStressPluginRegion.WikiMiruStressPluginRegionInput> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final String template;
    private final MiruSoyRenderer renderer;
    private final WikiMiruStressService stressService;
    private final Map<String, WikiMiruStressService.Stresser> stressers = new ConcurrentHashMap<>();

    public WikiMiruStressPluginRegion(String template,
        MiruSoyRenderer renderer,
        WikiMiruStressService stressService) {
        this.template = template;
        this.renderer = renderer;
        this.stressService = stressService;
    }

    public static class WikiMiruStressPluginRegionInput {

        final public String stresserId;
        final public String tenantId;
        final public int concurrency;
        final public int qps;
        final public String querier;
        final String queryPhrases;
        final public String queryPhraseFile;
        final public String action;

        public WikiMiruStressPluginRegionInput(String stresserId,
            String tenantId,
            int concurrency,
            int batchSize,
            String querier,
            String queryPhrases,
            String queryPhraseFile,
            String action) {

            this.stresserId = stresserId;
            this.tenantId = tenantId;
            this.concurrency = concurrency;
            this.qps = batchSize;
            this.querier = querier;
            this.queryPhrases = queryPhrases;
            this.queryPhraseFile = queryPhraseFile;
            this.action = action;
        }

    }

    @Override
    public String render(WikiMiruStressPluginRegionInput input) {
        Map<String, Object> data = Maps.newHashMap();
        try {

            if (input.action.equals("start")) {

                List<String> phrases = Lists.newArrayList();
                if (input.queryPhraseFile != null && !input.queryPhraseFile.trim().isEmpty()) {
                    Stream<String> stream = Files.lines(new File(input.queryPhraseFile.trim()).toPath());
                    stream.map(s -> s.split(",")[1].trim()).forEach(phrases::add);
                    phrases.addAll(stream.collect(Collectors.toList()));
                }

                if (input.queryPhraseFile != null && !input.queryPhraseFile.trim().isEmpty()) {
                    phrases.addAll(Lists.newArrayList(Splitter.on(",").omitEmptyStrings().trimResults().split(input.queryPhrases)));
                }

                List<String> tenantIds = Lists.newArrayList(Splitter.on(",").trimResults().omitEmptyStrings().split(input.tenantId));

                for (String tenantId : tenantIds) {

                    for (int i = 0; i < input.concurrency; i++) {

                        WikiMiruStressService.Stresser s = stressService.stress(tenantId, phrases, input);
                        stressers.put(s.stresserId, s);
                        Executors.newSingleThreadExecutor().submit(() -> {
                            try {
                                s.start();
                                return null;
                            } catch (Throwable x) {
                                s.message = "failed: " + x.getMessage();
                                LOG.error("Wiki oops", x);
                                return null;
                            }
                        });
                    }
                }


            }

            if (input.action.equals("stop")) {
                WikiMiruStressService.Stresser s = stressers.get(input.stresserId);
                if (s != null) {
                    s.running.set(false);
                }
            }

            if (input.action.equals("remove")) {
                WikiMiruStressService.Stresser s = stressers.remove(input.stresserId);
                if (s != null) {
                    s.running.set(false);
                }
            }

            List<Map<String, String>> rows = new ArrayList<>();
            for (WikiMiruStressService.Stresser s : stressers.values()) {
                Map<String, String> m = new HashMap<>();
                m.put("message", s.message);
                m.put("stresserId", s.stresserId);
                m.put("running", s.running.toString());
                m.put("failed", s.failed.toString());
                m.put("queried", s.queried.toString());
                m.put("latency",
                    "P50:" + s.statistics.getPercentile(50) +
                        " P75:" + s.statistics.getPercentile(75) +
                        " P90:" + s.statistics.getPercentile(90) +
                        " P95:" + s.statistics.getPercentile(95) +
                        " P99:" + s.statistics.getPercentile(99) +
                        " P99.9:" + s.statistics.getPercentile(99.9));
                m.put("tenantId", s.tenantId);
                m.put("elapse", String.valueOf(System.currentTimeMillis() - s.startTimestampMillis));

                rows.add(m);
            }
            data.put("stressers", rows);

        } catch (Exception e) {
            LOG.error("Unable to retrieve data", e);
        }

        return renderer.render(template, data);
    }

    @Override
    public String getTitle() {
        return "Wiki Stresser";
    }

}
