package com.jivesoftware.os.miru.sea.anomaly.deployable.region;

import com.jivesoftware.os.miru.sea.anomaly.deployable.MiruSoyRenderer;
import java.util.Collections;

// soy.sea.anomaly.chrome.headerRegion
public class MiruHeaderRegion implements Region<Void> {

    private final String template;
    private final MiruSoyRenderer renderer;

    public MiruHeaderRegion(String template, MiruSoyRenderer renderer) {
        this.template = template;
        this.renderer = renderer;
    }

    @Override
    public String render(Void input) {
        return renderer.render(template, Collections.<String, Object>emptyMap());
    }
}
