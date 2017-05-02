package com.jivesoftware.os.miru.siphon.deployable.region;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.query.siphon.MiruSiphonPlugin;
import com.jivesoftware.os.miru.siphon.deployable.siphoner.AmzaSiphoner;
import com.jivesoftware.os.miru.siphon.deployable.siphoner.AmzaSiphonerConfig;
import com.jivesoftware.os.miru.siphon.deployable.siphoner.AmzaSiphonerConfigStorage;
import com.jivesoftware.os.miru.siphon.deployable.siphoner.AmzaSiphoners;
import com.jivesoftware.os.miru.siphon.deployable.siphoner.MiruSiphonPluginRegistry;
import com.jivesoftware.os.miru.ui.MiruPageRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 *
 */
public class MiruSiphonPluginRegion implements MiruPageRegion<MiruSiphonPluginRegionInput> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final OrderIdProvider orderIdProvider;
    private final String template;
    private final MiruSoyRenderer renderer;
    private final MiruSiphonPluginRegistry siphonPluginRegistry;
    private final AmzaSiphonerConfigStorage amzaSiphonerConfigStorage;
    private final AmzaSiphoners siphoners;
    private final ObjectMapper mapper;

    public MiruSiphonPluginRegion(OrderIdProvider orderIdProvider,
        String template,
        MiruSiphonPluginRegistry siphonPluginRegistry,
        AmzaSiphonerConfigStorage amzaSiphonerConfigStorage,
        AmzaSiphoners siphoners,
        MiruSoyRenderer renderer,
        ObjectMapper mapper) {
        this.orderIdProvider = orderIdProvider;

        this.template = template;
        this.siphonPluginRegistry = siphonPluginRegistry;
        this.amzaSiphonerConfigStorage = amzaSiphonerConfigStorage;
        this.siphoners = siphoners;
        this.renderer = renderer;
        this.mapper = mapper;
    }

    @Override
    public String render(MiruSiphonPluginRegionInput input) {

        Map<String, Object> data = Maps.newHashMap();
        try {

            if (input.action.equals("add")) {
                long uniqueId = orderIdProvider.nextId();
                amzaSiphonerConfigStorage.multiPut(ImmutableMap.of(String.valueOf(uniqueId), new AmzaSiphonerConfig(uniqueId,
                    input.name,
                    input.description,
                    input.siphonPluginName,
                    new PartitionName(false, input.ringName.getBytes(StandardCharsets.UTF_8), input.partitionName.getBytes(StandardCharsets.UTF_8)),
                    new MiruTenantId(input.destinationTenantId.getBytes(StandardCharsets.UTF_8)),
                    input.batchSize)));
            }
            if (input.action.equals("remove")) {
                amzaSiphonerConfigStorage.multiRemove(Arrays.asList(String.valueOf(input.uniqueId)));
            }

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
