package com.jivesoftware.os.miru.stumptown.deployable;

import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.stumptown.deployable.region.MiruHeaderRegion;
import com.jivesoftware.os.miru.stumptown.deployable.region.MiruHomeRegion.HomeInput;
import com.jivesoftware.os.miru.stumptown.deployable.region.MiruManagePlugin;
import com.jivesoftware.os.miru.stumptown.deployable.region.StumptownChromeRegion;
import com.jivesoftware.os.miru.ui.MiruPageRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import java.util.List;

/**
 *
 */
public class MiruStumptownService {

    private final MiruSoyRenderer renderer;
    private final MiruHeaderRegion headerRegion;
    private final MiruPageRegion<HomeInput> homeRegion;

    private final List<MiruManagePlugin> plugins = Lists.newCopyOnWriteArrayList();

    public MiruStumptownService(
        MiruSoyRenderer renderer,
        MiruHeaderRegion headerRegion,
        MiruPageRegion<HomeInput> homeRegion
    ) {
        this.renderer = renderer;
        this.headerRegion = headerRegion;
        this.homeRegion = homeRegion;

    }

    public String render(String intakeURL) {
        return chrome(homeRegion).render(new HomeInput(intakeURL));
    }

    public void registerPlugin(MiruManagePlugin plugin) {
        plugins.add(plugin);
    }

    private <I, R extends MiruPageRegion<I>> StumptownChromeRegion<I, R> chrome(R region) {
        return new StumptownChromeRegion<>("soy.stumptown.chrome.chromeRegion", renderer, headerRegion, plugins, region);
    }

    public <I> String renderPlugin(MiruPageRegion<I> pluginRegion, I input) {
        return chrome(pluginRegion).render(input);
    }
}
