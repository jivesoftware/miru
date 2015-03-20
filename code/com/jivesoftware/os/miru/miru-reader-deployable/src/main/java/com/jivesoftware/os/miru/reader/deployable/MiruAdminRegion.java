package com.jivesoftware.os.miru.reader.deployable;

import java.util.Collections;

/**
 *
 */
public class MiruAdminRegion implements MiruPageRegion<Void> {

    private final String template;
    private final MiruSoyRenderer renderer;

    public MiruAdminRegion(String template, MiruSoyRenderer renderer) {
        this.template = template;
        this.renderer = renderer;
    }

    @Override
    public String render(Void input) {
        return renderer.render(template, Collections.<String, Object>emptyMap());
    }

    @Override
    public String getTitle() {
        return "Status";
    }
}
