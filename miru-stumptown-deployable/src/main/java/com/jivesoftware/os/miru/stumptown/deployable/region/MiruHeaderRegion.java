package com.jivesoftware.os.miru.stumptown.deployable.region;

import com.jivesoftware.os.miru.ui.MiruRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import java.util.Map;

// soy.stumptown.chrome.headerRegion
public class MiruHeaderRegion implements MiruRegion<Map<String, ?>> {

    private final String template;
    private final MiruSoyRenderer renderer;

    public MiruHeaderRegion(String template, MiruSoyRenderer renderer) {
        this.template = template;
        this.renderer = renderer;
    }

    @Override
    public String render(Map<String, ?> input) {
        return renderer.render(template, input);
    }
}
