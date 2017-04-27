package com.jivesoftware.os.miru.siphon.deployable.region;

import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.ui.MiruPageRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.miru.siphon.deployable.region.MiruHomeRegion.HomeInput;
import java.util.Map;

/**
 *
 */
public class MiruHomeRegion implements MiruPageRegion<HomeInput> {

    private final String template;
    private final MiruSoyRenderer renderer;

    public MiruHomeRegion(String template, MiruSoyRenderer renderer) {
        this.template = template;
        this.renderer = renderer;
    }

    public static class HomeInput {


        public HomeInput() {
        }
    }

    @Override
    public String render(HomeInput input) {
        Map<String, Object> data = Maps.newHashMap();
        return renderer.render(template, data);
    }

    @Override
    public String getTitle() {
        return "Home";
    }
}
