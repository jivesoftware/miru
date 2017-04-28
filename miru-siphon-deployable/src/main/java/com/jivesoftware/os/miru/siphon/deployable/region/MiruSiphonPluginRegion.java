package com.jivesoftware.os.miru.siphon.deployable.region;

import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.siphon.deployable.MiruSiphonUIService;
import com.jivesoftware.os.miru.ui.MiruPageRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Map;

/**
 *
 */
public class MiruSiphonPluginRegion implements MiruPageRegion<MiruSiphonPluginRegionInput> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final String template;
    private final MiruSoyRenderer renderer;
    private final MiruSiphonUIService miruSiphonUIService;

    public MiruSiphonPluginRegion(String template,
        MiruSiphonUIService miruSiphonUIService,
        MiruSoyRenderer renderer) {

        this.template = template;
        this.miruSiphonUIService = miruSiphonUIService;
        this.renderer = renderer;
    }

    @Override
    public String render(MiruSiphonPluginRegionInput input) {

        return renderer.render(template, query(input));
    }

    private Map<String, Object> query(MiruSiphonPluginRegionInput input) {
        Map<String, Object> data = Maps.newHashMap();
        try {
            return data;

        } catch (Exception e) {
            LOG.error("Unable to retrieve data", e);
            return null;
        }
    }

    @Override
    public String getTitle() {
        return "Miru Siphon";
    }
}
