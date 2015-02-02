package com.jivesoftware.os.miru.stumptown.deployable.region;

import com.jivesoftware.os.miru.stumptown.deployable.MiruSoyRenderer;
import java.util.Collections;

/**
 *
 */
public class MiruHomeRegion implements PageRegion<Void> {

    private final String template;
    private final MiruSoyRenderer renderer;

    public MiruHomeRegion(String template, MiruSoyRenderer renderer) {
        this.template = template;
        this.renderer = renderer;
    }

    @Override
    public String render(Void input) {
        return renderer.render(template, Collections.<String, Object>emptyMap());
    }

    @Override
    public String getTitle() {
        return "Home";
    }
}
