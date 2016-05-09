package com.jivesoftware.os.miru.anomaly.deployable;

import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.anomaly.deployable.region.AnomalyChromeRegion;
import com.jivesoftware.os.miru.anomaly.deployable.region.AnomalyPlugin;
import com.jivesoftware.os.miru.anomaly.deployable.region.MiruHeaderRegion;
import com.jivesoftware.os.miru.anomaly.deployable.region.MiruHomeRegion.HomeInput;
import com.jivesoftware.os.miru.ui.MiruPageRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import java.util.List;

/**
 *
 */
public class MiruAnomalyService {

    private final MiruSoyRenderer renderer;
    private final MiruHeaderRegion headerRegion;
    private final MiruPageRegion<HomeInput> homeRegion;

    private final List<AnomalyPlugin> plugins = Lists.newCopyOnWriteArrayList();

    public MiruAnomalyService(
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

    public void registerPlugin(AnomalyPlugin plugin) {
        plugins.add(plugin);
    }

    private <I, R extends MiruPageRegion<I>> AnomalyChromeRegion<I, R> chrome(R region) {
        return new AnomalyChromeRegion<>("soy.anomaly.chrome.chromeRegion", renderer, headerRegion, plugins, region);
    }

    public <I> String renderPlugin(MiruPageRegion<I> pluginRegion, I input) {
        return chrome(pluginRegion).render(input);
    }
}
