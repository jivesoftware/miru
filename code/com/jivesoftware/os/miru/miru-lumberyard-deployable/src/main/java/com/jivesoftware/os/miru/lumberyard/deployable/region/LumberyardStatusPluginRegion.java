package com.jivesoftware.os.miru.lumberyard.deployable.region;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.lumberyard.deployable.MiruSoyRenderer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Arrays;
import java.util.Map;

/**
 *
 */
// soy.miru.page.lumberyardStatusPluginRegion
public class LumberyardStatusPluginRegion implements MiruPageRegion<Optional<LumberyardStatusPluginRegion.LumberyardStatusPluginRegionInput>> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final String template;
    private final MiruSoyRenderer renderer;

    public LumberyardStatusPluginRegion(String template,
        MiruSoyRenderer renderer) {
        this.template = template;
        this.renderer = renderer;
    }

    public static class LumberyardStatusPluginRegionInput {

        final String foo;

        public LumberyardStatusPluginRegionInput(String foo) {

            this.foo = foo;
        }

    }

    @Override
    public String render(Optional<LumberyardStatusPluginRegionInput> optionalInput) {
        Map<String, Object> data = Maps.newHashMap();
        try {
            if (optionalInput.isPresent()) {
                LumberyardStatusPluginRegionInput input = optionalInput.get();

                data.put("status", Arrays.asList(new String[] {
                    "Todo build up lumberyard stats:",
                    "i.e ingressed:BLA" }));

            }
        } catch (Exception e) {
            log.error("Unable to retrieve data", e);
        }

        return renderer.render(template, data);
    }

    @Override
    public String getTitle() {
        return "Status";
    }
}
