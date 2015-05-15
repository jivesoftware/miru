package com.jivesoftware.os.miru.writer.deployable.region;

import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.ui.MiruPageRegion;
import com.jivesoftware.os.miru.ui.MiruRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import java.util.Map;

/**
 *
 */
// soy.miru.frame.chromeRegion
public class MiruFrameRegion<I, R extends MiruPageRegion<I>> implements MiruRegion<I> {

    private final String template;
    private final MiruSoyRenderer renderer;
    private final R region;

    public MiruFrameRegion(String template, MiruSoyRenderer renderer, R region) {
        this.template = template;
        this.renderer = renderer;
        this.region = region;
    }

    @Override
    public String render(I input) {
        Map<String, Object> data = Maps.newHashMap();
        data.put("region", region.render(input));
        data.put("title", region.getTitle());
        return renderer.render(template, data);
    }

}
