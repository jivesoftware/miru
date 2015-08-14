package com.jivesoftware.os.miru.tools.deployable;

import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.tools.deployable.region.MiruChromeRegion;
import com.jivesoftware.os.miru.tools.deployable.region.MiruFrameRegion;
import com.jivesoftware.os.miru.tools.deployable.region.MiruHeaderRegion;
import com.jivesoftware.os.miru.tools.deployable.region.MiruToolsPlugin;
import com.jivesoftware.os.miru.ui.MiruPageRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.routing.bird.shared.TenantRoutingProvider;
import java.util.List;

/**
 *
 */
public class MiruToolsService {

    private final MiruSoyRenderer renderer;
    private final MiruHeaderRegion headerRegion;
    private final MiruAdminRegion adminRegion;
    private final TenantRoutingProvider tenantRoutingProvider;

    private final List<MiruToolsPlugin> plugins = Lists.newCopyOnWriteArrayList();

    public MiruToolsService(
        MiruSoyRenderer renderer,
        MiruHeaderRegion headerRegion,
        MiruAdminRegion adminRegion,
        TenantRoutingProvider tenantRoutingProvider) {
        this.renderer = renderer;
        this.headerRegion = headerRegion;
        this.adminRegion = adminRegion;
        this.tenantRoutingProvider = tenantRoutingProvider;
    }

    public void registerPlugin(MiruToolsPlugin plugin) {
        plugins.add(plugin);
    }

    private <I, R extends MiruPageRegion<I>> MiruChromeRegion<I, R> chrome(R region) {
        return new MiruChromeRegion<>("soy.miru.chrome.chromeRegion", renderer, headerRegion, plugins, region, tenantRoutingProvider);
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
