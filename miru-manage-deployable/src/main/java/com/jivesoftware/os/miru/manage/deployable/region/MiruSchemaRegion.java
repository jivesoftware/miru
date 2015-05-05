package com.jivesoftware.os.miru.manage.deployable.region;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.manage.deployable.MiruSoyRenderer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.List;
import java.util.Map;

/**
 *
 */
// soy.miru.page.schemaRegion
public class MiruSchemaRegion implements MiruPageRegion<Optional<String>> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final String template;
    private final MiruSoyRenderer renderer;
    private final MiruClusterRegistry clusterRegistry;
    private final MiruWALClient miruWALClient;

    public MiruSchemaRegion(String template,
        MiruSoyRenderer renderer,
        MiruClusterRegistry clusterRegistry,
        MiruWALClient miruWALClient) {
        this.template = template;
        this.renderer = renderer;
        this.clusterRegistry = clusterRegistry;
        this.miruWALClient = miruWALClient;
    }

    @Override
    public String render(Optional<String> optionalSchema) {
        Map<String, Object> data = Maps.newHashMap();
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            if (optionalSchema.isPresent()) {
                JsonNode schemaNode = objectMapper.readValue(optionalSchema.get(), JsonNode.class);
                List<MiruTenantId> tenantIds = miruWALClient.getAllTenantIds();
                int missingCount = 0;
                List<MiruTenantId> matching = Lists.newArrayList();
                for (MiruTenantId tenantId : tenantIds) {
                    MiruSchema schema = clusterRegistry.getSchema(tenantId);
                    if (schema != null) {
                        JsonNode checkNode = objectMapper.valueToTree(schema);
                        if (schemaNode.equals(checkNode)) {
                            matching.add(tenantId);
                        }
                    } else {
                        missingCount++;
                    }
                }
                data.put("matching", Joiner.on(", ").join(matching));
                data.put("missingCount", missingCount);
            }
        } catch (Exception e) {
            log.error("Unable to retrieve data");
        }

        return renderer.render(template, data);
    }

    @Override
    public String getTitle() {
        return "Schema";
    }
}
