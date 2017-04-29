package com.jivesoftware.os.miru.siphon.deployable.region;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.miru.siphon.deployable.siphoner.AmzaSiphoner;
import com.jivesoftware.os.miru.siphon.deployable.siphoner.AmzaSiphoners;
import com.jivesoftware.os.miru.ui.MiruPageRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 *
 */
public class MiruSiphonPluginRegion implements MiruPageRegion<MiruSiphonPluginRegionInput> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final String template;
    private final MiruSoyRenderer renderer;
    private final AmzaSiphoners siphoners;

    public MiruSiphonPluginRegion(String template,
        AmzaSiphoners siphoners,
        MiruSoyRenderer renderer) {

        this.template = template;
        this.siphoners = siphoners;
        this.renderer = renderer;
    }

    @Override
    public String render(MiruSiphonPluginRegionInput input) {

        Map<String, Object> data = Maps.newHashMap();
        try {

            List<Map<String, String>> siphonsData = Lists.newArrayList();
            for (Entry<String, AmzaSiphoner> siphon : siphoners.all()) {
                Map<String, String> siphonData = Maps.newHashMap();

                String key = siphon.getKey();
                siphonData.put("id", key);
                AmzaSiphoner amzaSiphoner = siphon.getValue();
                siphonData.put("plugin", amzaSiphoner.siphonerConfig.siphonPluginName);
                siphonData.put("partition", PartitionName.toHumanReadableString(amzaSiphoner.siphonerConfig.partitionName));
                siphonData.put("called", amzaSiphoner.called.toString());
                siphonData.put("failed", amzaSiphoner.failed.toString());
                siphonData.put("siphoned", amzaSiphoner.siphoned.toString());
                siphonData.put("flushed", amzaSiphoner.flushed.toString());
                siphonsData.add(siphonData);
            }
            data.put("siphons", siphonsData);

        } catch (Exception e) {
            LOG.error("Unable to retrieve data", e);
            return null;
        }
        return renderer.render(template, data);
    }


    @Override
    public String getTitle() {
        return "Miru Siphon";
    }
}
