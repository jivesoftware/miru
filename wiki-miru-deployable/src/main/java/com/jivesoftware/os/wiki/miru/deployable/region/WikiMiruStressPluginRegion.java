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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

/**
 *
 */
public class WikiMiruStressPluginRegion implements MiruPageRegion<WikiMiruStressPluginRegion.WikiMiruStressPluginRegionInput> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final String template;
    private final MiruSoyRenderer renderer;
    private final WikiMiruStressService stressService;
    private final Map<String, WikiMiruStressService.Stresser> stressers = Maps.newConcurrentMap();

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
        final public int numberOfResult;
        final public boolean wildcardExpansion;

        final public String queryPhrases;
        final public String queryPhraseFile;
        final public String action;

        public WikiMiruStressPluginRegionInput(String stresserId,
            String tenantId,
            int concurrency,
            int batchSize,
            String querier,
            int numberOfResult,
            boolean wildcardExpansion,
            String queryPhrases,
            String queryPhraseFile,
            String action) {

            this.stresserId = stresserId;
            this.tenantId = tenantId;
            this.concurrency = concurrency;
            this.qps = batchSize;
            this.querier = querier;
            this.numberOfResult = numberOfResult;
            this.wildcardExpansion = wildcardExpansion;
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
                }

                if (input.queryPhrases != null && !input.queryPhrases.trim().isEmpty()) {
                    phrases.addAll(Lists.newArrayList(Splitter.on(",").omitEmptyStrings().trimResults().split(input.queryPhrases)));
                }

                if (!phrases.isEmpty()) {
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
                m.put("querier", s.input.querier);
                m.put("failed", s.failed.toString());
                m.put("queried", s.queried.toString());
                m.put("qps", "" + (int) (1000d / (s.totalQueryTime.get() / (double) s.queried.get())));

                m.put("amzaGets", s.amzaGets.toString());
                m.put("amzaGps", "" + (int) (1000d / (s.totalAmzaTime.get() / (double) s.amzaGets.get())));

                m.put("latencyP50", "" + (int)s.statistics.getPercentile(50));
                m.put("latencyP75", "" + (int)s.statistics.getPercentile(75));
                m.put("latencyP90", "" + (int)s.statistics.getPercentile(90));
                m.put("latencyP95", "" + (int)s.statistics.getPercentile(95));
                m.put("latencyP99", "" + (int)s.statistics.getPercentile(99));
                m.put("latencyP999", "" + (int)s.statistics.getPercentile(99.9));
                m.put("tenantId", s.tenantId);
                m.put("elapse", String.valueOf(System.currentTimeMillis() - s.startTimestampMillis));

                rows.add(m);
            }

            Collections.sort(rows, new Comparator<Map<String,String>>() {
                @Override
                public int compare(Map<String, String> o1, Map<String, String> o2) {
                    int i = o1.get("querier").compareTo(o2.get("querier"));
                    if (i != 0) {
                        return i;
                    }
                    i = Long.compare(Long.parseLong(o1.get("latencyP50")), Long.parseLong(o2.get("latencyP50")));
                    if (i != 0) {
                        return i;
                    }
                    return Long.compare(Long.parseLong(o1.get("stresserId")), Long.parseLong(o2.get("stresserId")));
                }
            });


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
