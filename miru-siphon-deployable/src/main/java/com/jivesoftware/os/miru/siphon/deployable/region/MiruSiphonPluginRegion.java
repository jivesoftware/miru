package com.jivesoftware.os.miru.siphon.deployable.region;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.query.siphon.MiruSiphonPlugin;
import com.jivesoftware.os.miru.siphon.deployable.siphoner.AmzaSiphoner;
import com.jivesoftware.os.miru.siphon.deployable.siphoner.AmzaSiphoners;
import com.jivesoftware.os.miru.siphon.deployable.siphoner.MiruSiphonPluginRegistry;
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
    private final MiruSiphonPluginRegistry siphonPluginRegistry;
    private final AmzaSiphoners siphoners;
    private final ObjectMapper mapper;

    public MiruSiphonPluginRegion(String template,
        MiruSiphonPluginRegistry siphonPluginRegistry,
        AmzaSiphoners siphoners,
        MiruSoyRenderer renderer,
        ObjectMapper mapper) {

        this.template = template;
        this.siphonPluginRegistry = siphonPluginRegistry;
        this.siphoners = siphoners;
        this.renderer = renderer;
        this.mapper = mapper;
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

            List<Map<String, String>> siphonPluginsData = Lists.newArrayList();
            for (String siphonPluginName : siphonPluginRegistry.allPluginNames()) {
                Map<String, String> siphonPluginData = Maps.newHashMap();
                siphonPluginData.put("name", siphonPluginName);

                MiruSiphonPlugin miruSiphonPlugin = siphonPluginRegistry.get(siphonPluginName);
                MiruSchema schema = miruSiphonPlugin.schema(new MiruTenantId("*".getBytes()));

                siphonPluginData.put("schema", mapper.writerWithDefaultPrettyPrinter().writeValueAsString(schema));


                siphonPluginsData.add(siphonPluginData);
            }
            data.put("siphonPlugins", siphonPluginsData);


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
