package com.jivesoftware.os.wiki.miru.deployable.region;

import com.jivesoftware.os.miru.ui.MiruRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import java.util.Map;

public class WikiMiruHeaderRegion implements MiruRegion<Map<String, ?>> {

    private final String template;
    private final MiruSoyRenderer renderer;

    public WikiMiruHeaderRegion(String template, MiruSoyRenderer renderer) {
        this.template = template;
        this.renderer = renderer;
    }

    @Override
    public String render(Map<String, ?> input) {
        return renderer.render(template, input);
    }
}
