package com.jivesoftware.os.wiki.miru.deployable.region;

import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.ui.MiruPageRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.wiki.miru.deployable.WikiMiruIndexService;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

/**
 *
 */
// soy.stumptown.page.stumptownStatusPluginRegion
public class WikiMiruIndexPluginRegion implements MiruPageRegion<WikiMiruIndexPluginRegion.WikiMiruIndexPluginRegionInput> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final String template;
    private final MiruSoyRenderer renderer;
    private final WikiMiruIndexService indexService;
    private final Map<String, WikiMiruIndexService.Indexer> indexers = new ConcurrentHashMap<>();

    public WikiMiruIndexPluginRegion(String template,
        MiruSoyRenderer renderer,
        WikiMiruIndexService indexService) {
        this.template = template;
        this.renderer = renderer;
        this.indexService = indexService;
    }

    public static class WikiMiruIndexPluginRegionInput {

        final String indexerId;
        final String tenantId;
        final String wikiDumpFile;
        final int batchSize;
        final boolean miruEnabled;
        final String esClusterName;
        final List<String> esHosts;
        final String action;

        public WikiMiruIndexPluginRegionInput(String indexerId,
            String tenantId,
            String wikiDumpFile,
            int batchSize,
            boolean miruEnabled,
            String esClusterName,
            List<String> esHosts,
            String action) {

            this.indexerId = indexerId;
            this.tenantId = tenantId;
            this.wikiDumpFile = wikiDumpFile;
            this.batchSize = batchSize;
            this.miruEnabled = miruEnabled;
            this.esClusterName = esClusterName;
            this.esHosts = esHosts;
            this.action = action;
        }

    }

    @Override
    public String render(WikiMiruIndexPluginRegionInput input) {
        Map<String, Object> data = Maps.newHashMap();
        try {

            if (input.action.equals("start")) {
                WikiMiruIndexService.Indexer i = indexService.index(input.tenantId, input.wikiDumpFile, input.batchSize, input.miruEnabled, input.esClusterName, input.esHosts);
                indexers.put(i.indexerId, i);
                Executors.newSingleThreadExecutor().submit(() -> {
                    try {
                        i.start();
                        return null;
                    } catch (Throwable x) {
                        i.message = "failed: "+x.getMessage();
                        LOG.error("Wiki oops", x);
                        return null;
                    }
                });
            }

            if (input.action.equals("stop")) {
                WikiMiruIndexService.Indexer i = indexers.get(input.indexerId);
                if (i != null) {
                    i.running.set(false);
                }
            }

            if (input.action.equals("remove")) {
                WikiMiruIndexService.Indexer i = indexers.remove(input.indexerId);
                if (i != null) {
                    i.running.set(false);
                }
            }

            List<Map<String, String>> rows = new ArrayList<>();
            for (WikiMiruIndexService.Indexer i : indexers.values()) {
                Map<String, String> m = new HashMap<>();
                m.put("message", i.message);
                m.put("indexerId", i.indexerId);
                m.put("running", i.running.toString());
                m.put("indexed", i.indexed.toString());
                m.put("tenantId", i.tenantId);
                m.put("pathToWikiDumpFile", i.pathToWikiDumpFile);
                m.put("elapse", String.valueOf(System.currentTimeMillis() - i.startTimestampMillis));

                rows.add(m);
            }
            data.put("indexers", rows);

        } catch (Exception e) {
            LOG.error("Unable to retrieve data", e);
        }

        return renderer.render(template, data);
    }

    @Override
    public String getTitle() {
        return "Wiki Indexer";
    }

}
