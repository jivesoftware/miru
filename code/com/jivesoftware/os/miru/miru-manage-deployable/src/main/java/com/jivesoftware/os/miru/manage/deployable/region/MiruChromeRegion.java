package com.jivesoftware.os.miru.manage.deployable.region;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.manage.deployable.MiruSoyRenderer;
import java.util.List;
import java.util.Map;

/**
 *
 */
// soy.miru.chrome.chromeRegion
public class MiruChromeRegion<I, R extends MiruPageRegion<I>> implements MiruRegion<I> {

    private final String template;
    private final MiruSoyRenderer renderer;
    private final MiruHeaderRegion headerRegion;
    private final List<MiruManagePlugin> plugins;
    private final R region;

    public MiruChromeRegion(String template, MiruSoyRenderer renderer, MiruHeaderRegion headerRegion, List<MiruManagePlugin> plugins, R region) {
        this.template = template;
        this.renderer = renderer;
        this.headerRegion = headerRegion;
        this.plugins = plugins;
        this.region = region;
    }

    @Override
    public String render(I input) {
        Map<String, Object> data = Maps.newHashMap();
        data.put("header", headerRegion.render(null));
        data.put("region", region.render(input));
        data.put("title", region.getTitle());
        data.put("plugins", Lists.transform(plugins, new Function<MiruManagePlugin, Map<String, String>>() {
            @Override
            public Map<String, String> apply(MiruManagePlugin input) {
                return ImmutableMap.of("name", input.name, "path", input.path);
            }
        }));
        return renderer.render(template, data);

        /*
        // inject js page region module data
        List<String> jsmodulesVal = Arrays.asList(JSProcessor.classToAMDPath(region.getClass()));

        context.put("jsmodules").value(jsmodulesVal);
        */
    }

}
