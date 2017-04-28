package com.jivesoftware.os.miru.siphon.deployable;

import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.siphon.deployable.region.MiruSiphonHomeRegion.HomeInput;
import com.jivesoftware.os.miru.siphon.deployable.region.MiruSiphonUIPlugin;
import com.jivesoftware.os.miru.siphon.deployable.region.MiruSiphonHeaderRegion;
import com.jivesoftware.os.miru.ui.MiruPageRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.miru.siphon.deployable.region.MiruSiphonChromeRegion;
import java.util.List;

/**
 *
 */
public class MiruSiphonUIService {

    private final MiruSoyRenderer renderer;
    private final MiruSiphonHeaderRegion headerRegion;
    private final MiruPageRegion<HomeInput> homeRegion;

    private final List<MiruSiphonUIPlugin> plugins = Lists.newCopyOnWriteArrayList();

    public MiruSiphonUIService(
        MiruSoyRenderer renderer,
        MiruSiphonHeaderRegion headerRegion,
        MiruPageRegion<HomeInput> homeRegion
    ) {
        this.renderer = renderer;
        this.headerRegion = headerRegion;
        this.homeRegion = homeRegion;

    }

    public String render() {
        return chrome(homeRegion).render(new HomeInput());
    }

    public void registerPlugin(MiruSiphonUIPlugin plugin) {
        plugins.add(plugin);
    }

    private <I, R extends MiruPageRegion<I>> MiruSiphonChromeRegion<I, R> chrome(R region) {
        return new MiruSiphonChromeRegion<>("soy.wikimiru.chrome.chromeRegion", renderer, headerRegion, plugins, region);
    }

    public <I> String renderPlugin(MiruPageRegion<I> pluginRegion, I input) {
        return chrome(pluginRegion).render(input);
    }
}
