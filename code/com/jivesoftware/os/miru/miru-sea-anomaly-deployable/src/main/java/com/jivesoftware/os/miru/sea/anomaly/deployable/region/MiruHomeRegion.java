package com.jivesoftware.os.miru.sea.anomaly.deployable.region;

import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.sea.anomaly.deployable.MiruSoyRenderer;
import com.jivesoftware.os.miru.sea.anomaly.deployable.region.MiruHomeRegion.HomeInput;
import java.util.Map;

/**
 *
 */
public class MiruHomeRegion implements PageRegion<HomeInput> {

    private final String template;
    private final MiruSoyRenderer renderer;

    public MiruHomeRegion(String template, MiruSoyRenderer renderer) {
        this.template = template;
        this.renderer = renderer;
    }
    
     public static class HomeInput {

        final String intakeURL;

        public HomeInput(String intakeURL) {
            this.intakeURL = intakeURL;
        }
    }

    @Override
    public String render(HomeInput input) {
        Map<String, Object> data = Maps.newHashMap();
        data.put("intakeURL", input.intakeURL);
        return renderer.render(template, data);
    }

    @Override
    public String getTitle() {
        return "Home";
    }
}
