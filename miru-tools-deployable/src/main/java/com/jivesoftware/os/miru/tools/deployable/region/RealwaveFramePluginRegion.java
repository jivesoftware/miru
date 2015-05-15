package com.jivesoftware.os.miru.tools.deployable.region;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.tools.deployable.region.RealwavePluginRegion.RealwavePluginRegionInput;
import com.jivesoftware.os.miru.ui.MiruPageRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import java.util.Map;

/**
 *
 */
// soy.miru.page.realwaveFramePluginRegion
public class RealwaveFramePluginRegion implements MiruPageRegion<Optional<RealwavePluginRegionInput>> {

    private final String template;
    private final MiruSoyRenderer renderer;

    public RealwaveFramePluginRegion(String template,
        MiruSoyRenderer renderer) {
        this.template = template;
        this.renderer = renderer;
    }

    @Override
    public String render(Optional<RealwavePluginRegionInput> optionalInput) {
        Map<String, Object> data = Maps.newHashMap();
        if (optionalInput.isPresent()) {
            RealwavePluginRegionInput input = optionalInput.get();

            data.put("tenant", input.tenant);
            data.put("lookbackSeconds", String.valueOf(input.lookbackSeconds));
            data.put("buckets", String.valueOf(input.buckets));
            data.put("field1", input.field1);
            data.put("terms1", input.terms1);
            data.put("field2", input.field2);
            data.put("terms2", input.terms2);
            data.put("filters", input.filters);
            data.put("graphType", input.graphType);
            data.put("legend", input.legend);
            data.put("width", input.width);
            data.put("height", input.height);
            data.put("requireFocus", input.requireFocus);
        }

        return renderer.render(template, data);
    }

    @Override
    public String getTitle() {
        return "Realwave";
    }
}
