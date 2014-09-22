package com.jivesoftware.os.miru.plugin.schema;

import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;

/**
 *
 */
public class SingleSchemaProvider implements MiruSchemaProvider {

    private final MiruSchema schema;

    public SingleSchemaProvider(MiruSchema schema) {
        this.schema = schema;
    }

    @Override
    public MiruSchema getSchema(MiruTenantId miruTenantId) {
        return schema;
    }
}
