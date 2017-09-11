package com.jivesoftware.os.miru.sync.deployable;

import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.sync.deployable.region.MiruChromeRegion;
import com.jivesoftware.os.miru.sync.deployable.region.MiruHeaderRegion;
import com.jivesoftware.os.miru.sync.deployable.region.MiruStatusRegionInput;
import com.jivesoftware.os.miru.sync.deployable.region.MiruSyncPlugin;
import com.jivesoftware.os.miru.ui.MiruPageRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import java.util.List;

/**
 *
 */
public class MiruSyncUIService {

    private final MiruSoyRenderer renderer;
    private final MiruHeaderRegion headerRegion;
    private final MiruPageRegion<Void> adminRegion;
    private final MiruPageRegion<MiruStatusRegionInput> statusRegion;

    private final List<MiruSyncPlugin> plugins = Lists.newCopyOnWriteArrayList();

    public MiruSyncUIService(
        MiruSoyRenderer renderer,
        MiruHeaderRegion headerRegion,
        MiruPageRegion<Void> adminRegion,
        MiruPageRegion<MiruStatusRegionInput> statusRegion) {
        this.renderer = renderer;
        this.headerRegion = headerRegion;
        this.adminRegion = adminRegion;
        this.statusRegion = statusRegion;
    }

    private <I, R extends MiruPageRegion<I>> MiruChromeRegion<I, R> chrome(R region) {
        return new MiruChromeRegion<>("soy.miru.chrome.chromeRegion", renderer, headerRegion, plugins, region);
    }

    public String render(String redirUrl) {
        headerRegion.setRedirUrl(redirUrl);
        return chrome(adminRegion).render(null);
    }

    public String renderStatus(MiruStatusRegionInput input) {
        return chrome(statusRegion).render(input);
    }
}
