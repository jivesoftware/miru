package com.jivesoftware.os.wiki.miru.deployable.region;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.ui.MiruPageRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.http.client.HttpResponseMapper;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.wiki.miru.deployable.WikiMiruIndexService.Wiki;
import com.jivesoftware.os.wiki.miru.deployable.region.WikiWikiPluginRegion.WikiWikiPluginRegionInput;
import com.jivesoftware.os.wiki.miru.deployable.storage.WikiMiruPayloadsAmza;
import info.bliki.wiki.filter.HTMLConverter;
import info.bliki.wiki.model.WikiModel;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class WikiWikiPluginRegion implements MiruPageRegion<WikiWikiPluginRegionInput> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final String template;
    private final MiruSoyRenderer renderer;
    private final TenantAwareHttpClient<String> readerClient;
    private final ObjectMapper requestMapper;
    private final HttpResponseMapper responseMapper;
    private final WikiMiruPayloadsAmza payloads;

    public WikiWikiPluginRegion(String template,
        MiruSoyRenderer renderer,
        TenantAwareHttpClient<String> readerClient,
        ObjectMapper requestMapper,
        HttpResponseMapper responseMapper,
        WikiMiruPayloadsAmza payloads) {

        this.template = template;
        this.renderer = renderer;
        this.readerClient = readerClient;
        this.requestMapper = requestMapper;
        this.responseMapper = responseMapper;
        this.payloads = payloads;
    }

    public static class WikiWikiPluginRegionInput {

        final String tenantId;
        final String wikiId;

        public WikiWikiPluginRegionInput(String tenantId, String wikiId) {
            this.tenantId = tenantId;
            this.wikiId = wikiId;
        }
    }

    @Override
    public String render(WikiWikiPluginRegionInput input) {
        Map<String, Object> data = Maps.newHashMap();
        try {

            data.put("tenantId", input.tenantId);
            data.put("wikiId", input.wikiId);

            Wiki wiki = payloads.get(new MiruTenantId(input.tenantId.getBytes(StandardCharsets.UTF_8)), input.wikiId, Wiki.class);
            if (wiki != null) {

                data.put("id", input.wikiId);
                data.put("subject", wiki.subject);

                WikiModel wikiModel = new WikiModel("https://en.wikipedia.org/wiki/${image}", "https://en.wikipedia.org/wiki/${title}");
                String htmlBody = wikiModel.render(new HTMLConverter(), wiki.body);

                data.put("body", htmlBody);


                Wiki wikiTopics = payloads.get(new MiruTenantId(input.tenantId.getBytes(StandardCharsets.UTF_8)), input.wikiId + "-topics", Wiki.class);
                if (wikiTopics != null) {
                    data.put("topics", wikiTopics.body);
                }


            } else {
                data.put("id", input.wikiId);
                data.put("subject", "NOT FOUND");
                data.put("body", "NOT FOUND");
            }

        } catch (Exception e) {
            LOG.error("Unable to retrieve data", e);
        }
        return renderer.render(template, data);
    }

    @Override
    public String getTitle() {
        return "Wiki Wiki";
    }
}
