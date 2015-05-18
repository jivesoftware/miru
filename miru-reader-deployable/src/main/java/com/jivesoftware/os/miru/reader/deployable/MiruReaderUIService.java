package com.jivesoftware.os.miru.reader.deployable;

import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.ui.MiruPageRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import java.util.List;

/**
 *
 */
public class MiruReaderUIService {

    private final MiruSoyRenderer renderer;
    private final MiruHeaderRegion headerRegion;
    private final MiruAdminRegion adminRegion;

    private final List<MiruReaderUIPlugin> plugins = Lists.newCopyOnWriteArrayList();

    public MiruReaderUIService(
        MiruSoyRenderer renderer,
        MiruHeaderRegion headerRegion,
        MiruAdminRegion adminRegion) {
        this.renderer = renderer;
        this.headerRegion = headerRegion;
        this.adminRegion = adminRegion;
    }

    public void registerPlugin(MiruReaderUIPlugin plugin) {
        plugins.add(plugin);
    }

    private <I, R extends MiruPageRegion<I>> MiruChromeRegion<I, R> chrome(R region) {
        return new MiruChromeRegion<>("soy.miru.chrome.chromeRegion", renderer, headerRegion, plugins, region);
    }

    private <I, R extends MiruPageRegion<I>> MiruFrameRegion<I, R> frame(R region) {
        return new MiruFrameRegion<>("soy.miru.frame.chromeRegion", renderer, region);
    }

    public String render() {
        return chrome(adminRegion).render(null);
    }

    public <I> String renderPlugin(MiruPageRegion<I> pluginRegion, I input) {
        return chrome(pluginRegion).render(input);
    }

    public <I> String renderFramePlugin(MiruPageRegion<I> pluginRegion, I input) {
        return frame(pluginRegion).render(input);
    }

}
