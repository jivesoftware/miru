package com.jivesoftware.os.miru.lumberyard.deployable;

import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.lumberyard.deployable.region.MiruChromeRegion;
import com.jivesoftware.os.miru.lumberyard.deployable.region.MiruHeaderRegion;
import com.jivesoftware.os.miru.lumberyard.deployable.region.MiruManagePlugin;
import com.jivesoftware.os.miru.lumberyard.deployable.region.MiruPageRegion;
import java.util.List;

/**
 *
 */
public class MiruQueryLumberyardService {

    private final MiruSoyRenderer renderer;
    private final MiruHeaderRegion headerRegion;
    private final MiruPageRegion<Void> adminRegion;

    private final List<MiruManagePlugin> plugins = Lists.newCopyOnWriteArrayList();

    public MiruQueryLumberyardService(
        MiruSoyRenderer renderer,
        MiruHeaderRegion headerRegion,
        MiruPageRegion<Void> adminRegion
    ) {
        this.renderer = renderer;
        this.headerRegion = headerRegion;
        this.adminRegion = adminRegion;

    }

    public String render() {
        return chrome(adminRegion).render(null);
    }

    public void registerPlugin(MiruManagePlugin plugin) {
        plugins.add(plugin);
    }

    private <I, R extends MiruPageRegion<I>> MiruChromeRegion<I, R> chrome(R region) {
        return new MiruChromeRegion<>("soy.miru.chrome.chromeRegion", renderer, headerRegion, plugins, region);
    }

    public <I> String renderPlugin(MiruPageRegion<I> pluginRegion, I input) {
        return chrome(pluginRegion).render(input);
    }
}
