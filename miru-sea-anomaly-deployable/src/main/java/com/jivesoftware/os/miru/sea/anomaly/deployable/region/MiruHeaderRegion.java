package com.jivesoftware.os.miru.sea.anomaly.deployable.region;

import com.jivesoftware.os.miru.ui.MiruRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import java.util.Map;

// soy.sea.anomaly.chrome.headerRegion
public class MiruHeaderRegion implements MiruRegion<Map<String, Object>> {

    private final String template;
    private final MiruSoyRenderer renderer;

    public MiruHeaderRegion(String template, MiruSoyRenderer renderer) {
        this.template = template;
        this.renderer = renderer;
    }

    @Override
    public String render(Map<String, Object> input) {
        return renderer.render(template, input);
    }
}
