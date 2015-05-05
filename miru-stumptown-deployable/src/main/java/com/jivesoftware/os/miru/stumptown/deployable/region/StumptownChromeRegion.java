package com.jivesoftware.os.miru.stumptown.deployable.region;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.stumptown.deployable.MiruSoyRenderer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
// soy.stumptown.chrome.chromeRegion
public class StumptownChromeRegion<I, R extends PageRegion<I>> implements Region<I> {

    private final String template;
    private final MiruSoyRenderer renderer;
    private final MiruHeaderRegion headerRegion;
    private final List<MiruManagePlugin> plugins;
    private final R region;

    public StumptownChromeRegion(String template, MiruSoyRenderer renderer, MiruHeaderRegion headerRegion, List<MiruManagePlugin> plugins, R region) {
        this.template = template;
        this.renderer = renderer;
        this.headerRegion = headerRegion;
        this.plugins = plugins;
        this.region = region;
    }

    @Override
    public String render(I input) {
        List<Map<String, String>> p = Lists.transform(plugins,
            plugin -> ImmutableMap.of("name", plugin.name, "path", plugin.path, "glyphicon", plugin.glyphicon));
        Map<String, Object> headerData = new HashMap<>();
        headerData.put("plugins", p);

        Map<String, Object> data = Maps.newHashMap();
        data.put("header", headerRegion.render(headerData));
        data.put("region", region.render(input));
        data.put("title", region.getTitle());
        data.put("plugins", plugins);
        return renderer.render(template, data);
    }

}
