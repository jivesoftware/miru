package com.jivesoftware.os.miru.wal.deployable.region;

import com.jivesoftware.os.miru.ui.MiruRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import java.util.Collections;

// soy.miru.chrome.headerRegion
public class MiruHeaderRegion implements MiruRegion<Void> {

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
