package com.jivesoftware.os.miru.manage.deployable.region;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.ui.MiruPageRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.List;
import java.util.Map;

/**
 *
 */
// soy.miru.page.schemaRegion
public class MiruSchemaRegion implements MiruPageRegion<MiruSchemaRegion.SchemaInput> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final String template;
    private final MiruSoyRenderer renderer;
    private final MiruClusterRegistry clusterRegistry;
    private final MiruWALClient<?, ?> miruWALClient;
    private final ObjectMapper mapper;

    public MiruSchemaRegion(String template,
        MiruSoyRenderer renderer,
        MiruClusterRegistry clusterRegistry,
        MiruWALClient<?, ?> miruWALClient,
        ObjectMapper mapper) {
        this.template = template;
        this.renderer = renderer;
        this.clusterRegistry = clusterRegistry;
        this.miruWALClient = miruWALClient;
        this.mapper = mapper;
    }

    public static class SchemaInput {

        public final MiruTenantId tenantId;
        public final String lookupName;
        public final int lookupVersion;

        public SchemaInput(MiruTenantId tenantId, String lookupName, int lookupVersion) {
            this.tenantId = tenantId;
            this.lookupName = lookupName;
            this.lookupVersion = lookupVersion;
        }
    }

    @Override
    public String render(SchemaInput input) {
        Map<String, Object> data = Maps.newHashMap();
        try {
            data.put("tenantId", input.tenantId != null ? input.tenantId.toString() : "");
            data.put("lookupName", input.lookupName != null ? input.lookupName : "");
            data.put("lookupVersion", input.lookupVersion != -1 ? String.valueOf(input.lookupVersion) : "");

            if (input.tenantId != null) {
                MiruSchema schema = clusterRegistry.getSchema(input.tenantId);
                data.put("tenantSchema", schema != null ? mapper.writerWithDefaultPrettyPrinter().writeValueAsString(schema) : "(unregistered)");
            }
            if (input.lookupName != null) {
                List<MiruTenantId> tenantIds = miruWALClient.getAllTenantIds();
                int missingCount = 0;
                List<MiruTenantId> matching = Lists.newArrayList();
                for (MiruTenantId tenantId : tenantIds) {
                    MiruSchema schema = clusterRegistry.getSchema(tenantId);
                    if (schema != null) {
                        if (schema.getName().equals(input.lookupName) && (input.lookupVersion == -1 || schema.getVersion() == input.lookupVersion)) {
                            matching.add(tenantId);
                        }
                    } else {
                        missingCount++;
                    }
                }
                data.put("lookupMatching", Joiner.on(", ").join(matching));
                data.put("lookupMissing", missingCount);
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
