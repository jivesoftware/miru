package com.jivesoftware.os.miru.catwalk.deployable.region;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.ui.MiruPageRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Map;

/**
 *
 */
// soy.miru.page.hostsRegion
public class MiruSomethingRegion implements MiruPageRegion<Optional<Void>> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final String template;
    private final MiruSoyRenderer renderer;

    public MiruSomethingRegion(String template,
        MiruSoyRenderer renderer) {
        this.template = template;
        this.renderer = renderer;
    }

    @Override
    public String render(Optional<Void> optionalInput) {
        Map<String, Object> data = Maps.newHashMap();
        try {

        } catch (Exception e) {
            log.error("Unable to retrieve data");
        }

        return renderer.render(template, data);
    }

    @Override
    public String getTitle() {
        return "Something";
    }
}
