package com.jivesoftware.os.miru.manage.deployable.region;

import com.fasterxml.jackson.databind.ObjectMapper;
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
        public final String action;
        public final boolean upgradeOnMissing;
        public final boolean upgradeOnError;

        public SchemaInput(MiruTenantId tenantId, String lookupName, int lookupVersion, String action, boolean upgradeOnMissing, boolean upgradeOnError) {
            this.tenantId = tenantId;
            this.lookupName = lookupName;
            this.lookupVersion = lookupVersion;
            this.action = action;
            this.upgradeOnMissing = upgradeOnMissing;
            this.upgradeOnError = upgradeOnError;
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

                if (input.action.equals("upgrade")) {
                    int count = clusterRegistry.upgradeSchema(schema, input.upgradeOnMissing, input.upgradeOnError);
                    data.put("tenantUpgrades", count);
                }
            } else if (input.lookupName != null) {
                MiruSchema matchingSchema = null;
                List<MiruTenantId> tenantIds = miruWALClient.getAllTenantIds();
                int matchingCount = 0;
                int missingCount = 0;
                int errorCount = 0;
                for (MiruTenantId tenantId : tenantIds) {
                    try {
                        MiruSchema schema = clusterRegistry.getSchema(tenantId);
                        if (schema != null &&
                            schema.getName().equals(input.lookupName) &&
                            (input.lookupVersion == -1 || schema.getVersion() == input.lookupVersion)) {
                            if (matchingSchema == null || schema.getVersion() > matchingSchema.getVersion()) {
                                matchingSchema = schema;
                            }
                            matchingCount++;
                        } else {
                            missingCount++;
                        }
                    } catch (Exception e) {
                        errorCount++;
                    }
                }
                data.put("lookupMatching", matchingCount);
                data.put("lookupMissing", missingCount);
                data.put("lookupErrors", errorCount);

                if (input.action.equals("upgrade")) {
                    int count = 0;
                    if (matchingSchema != null) {
                        count = clusterRegistry.upgradeSchema(matchingSchema, input.upgradeOnMissing, input.upgradeOnError);
                    }
                    data.put("lookupUpgrades", count);
                }
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
