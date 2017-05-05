package com.jivesoftware.os.miru.catwalk.deployable;

import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.catwalk.deployable.region.MiruCatwalkPlugin;
import com.jivesoftware.os.miru.catwalk.deployable.region.MiruChromeRegion;
import com.jivesoftware.os.miru.catwalk.deployable.region.MiruHeaderRegion;
import com.jivesoftware.os.miru.catwalk.deployable.region.MiruInspectRegion.InspectInput;
import com.jivesoftware.os.miru.ui.MiruPageRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import java.util.List;

/**
 *
 */
public class MiruCatwalkUIService {

    private final MiruSoyRenderer renderer;
    private final MiruHeaderRegion headerRegion;
    private final MiruPageRegion<Void> adminRegion;
    private final MiruPageRegion<InspectInput> inspectRegion;

    private final List<MiruCatwalkPlugin> plugins = Lists.newCopyOnWriteArrayList();

    public MiruCatwalkUIService(
        MiruSoyRenderer renderer,
        MiruHeaderRegion headerRegion,
        MiruPageRegion<Void> adminRegion,
        MiruPageRegion<InspectInput> inspectRegion) {
        this.renderer = renderer;
        this.headerRegion = headerRegion;
        this.adminRegion = adminRegion;
        this.inspectRegion = inspectRegion;
    }

    public void registerPlugin(MiruCatwalkPlugin plugin) {
        plugins.add(plugin);
    }

    private <I, R extends MiruPageRegion<I>> MiruChromeRegion<I, R> chrome(R region) {
        return new MiruChromeRegion<>("soy.miru.chrome.chromeRegion", renderer, headerRegion, plugins, region);
    }

    public String render(String redirUrl) {
        headerRegion.setRedirUrl(redirUrl);
        return chrome(adminRegion).render(null);
    }

    public String renderInspect(InspectInput input) {
        return chrome(inspectRegion).render(input);
    }

}
