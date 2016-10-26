package com.jivesoftware.os.wiki.miru.deployable;

import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.ui.MiruPageRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.wiki.miru.deployable.region.MiruHomeRegion.HomeInput;
import com.jivesoftware.os.wiki.miru.deployable.region.MiruManagePlugin;
import com.jivesoftware.os.wiki.miru.deployable.region.WikiMiruChromeRegion;
import com.jivesoftware.os.wiki.miru.deployable.region.WikiMiruHeaderRegion;
import java.util.List;

/**
 *
 */
public class WikiMiruService {

    private final MiruSoyRenderer renderer;
    private final WikiMiruHeaderRegion headerRegion;
    private final MiruPageRegion<HomeInput> homeRegion;

    private final List<MiruManagePlugin> plugins = Lists.newCopyOnWriteArrayList();

    public WikiMiruService(
        MiruSoyRenderer renderer,
        WikiMiruHeaderRegion headerRegion,
        MiruPageRegion<HomeInput> homeRegion
    ) {
        this.renderer = renderer;
        this.headerRegion = headerRegion;
        this.homeRegion = homeRegion;

    }

    public String render() {
        return chrome(homeRegion).render(new HomeInput());
    }

    public void registerPlugin(MiruManagePlugin plugin) {
        plugins.add(plugin);
    }

    private <I, R extends MiruPageRegion<I>> WikiMiruChromeRegion<I, R> chrome(R region) {
        return new WikiMiruChromeRegion<>("soy.wikimiru.chrome.chromeRegion", renderer, headerRegion, plugins, region);
    }

    public <I> String renderPlugin(MiruPageRegion<I> pluginRegion, I input) {
        return chrome(pluginRegion).render(input);
    }
}
