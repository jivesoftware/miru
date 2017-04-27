package com.jivesoftware.os.miru.siphon.deployable.region;

import com.jivesoftware.os.miru.ui.MiruRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import java.util.Map;

public class MiruSiphonHeaderRegion implements MiruRegion<Map<String, ?>> {

    private final String template;
    private final MiruSoyRenderer renderer;

    public MiruSiphonHeaderRegion(String template, MiruSoyRenderer renderer) {
        this.template = template;
        this.renderer = renderer;
    }

    @Override
    public String render(Map<String, ?> input) {
        return renderer.render(template, input);
    }
}
