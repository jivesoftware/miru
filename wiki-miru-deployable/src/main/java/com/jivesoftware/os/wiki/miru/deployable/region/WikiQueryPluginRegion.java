package com.jivesoftware.os.wiki.miru.deployable.region;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.plugin.query.LuceneBackedQueryParser;
import com.jivesoftware.os.miru.ui.MiruPageRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.http.client.HttpResponseMapper;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.wiki.miru.deployable.region.WikiQueryPluginRegion.WikiMiruPluginRegionInput;
import com.jivesoftware.os.wiki.miru.deployable.storage.WikiMiruPayloadStorage;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 */
// soy.stumptown.page.stumptownQueryPluginRegion
public class WikiQueryPluginRegion implements MiruPageRegion<WikiMiruPluginRegionInput> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final String template;
    private final MiruSoyRenderer renderer;
    private final TenantAwareHttpClient<String> readerClient;
    private final ObjectMapper requestMapper;
    private final HttpResponseMapper responseMapper;
    private final WikiMiruPayloadStorage payloads;

    private final LuceneBackedQueryParser subjectQueryParser = new LuceneBackedQueryParser("subject");
    private final LuceneBackedQueryParser bodyQueryParser = new LuceneBackedQueryParser("body");

    public WikiQueryPluginRegion(String template,
        MiruSoyRenderer renderer,
        TenantAwareHttpClient<String> readerClient,
        ObjectMapper requestMapper,
        HttpResponseMapper responseMapper,
        WikiMiruPayloadStorage payloads) {

        this.template = template;
        this.renderer = renderer;
        this.readerClient = readerClient;
        this.requestMapper = requestMapper;
        this.responseMapper = responseMapper;
        this.payloads = payloads;
    }

    public static class WikiMiruPluginRegionInput {

        final String subject;
        final String body;

        public WikiMiruPluginRegionInput(String subject, String body) {
            this.subject = subject;
            this.body = body;
        }
    }

    @Override
    public String render(WikiMiruPluginRegionInput input) {
        Map<String, Object> data = Maps.newHashMap();
        try {

            data.put("subject", input.subject);
            data.put("body", input.body);

            List<String> results = new ArrayList<>();
            results.add("todo");

            data.put("results", results);

        } catch (Exception e) {
            LOG.error("Unable to retrieve data", e);
        }
        return renderer.render(template, data);
    }

    @Override
    public String getTitle() {
        return "Wiki Query";
    }
}
