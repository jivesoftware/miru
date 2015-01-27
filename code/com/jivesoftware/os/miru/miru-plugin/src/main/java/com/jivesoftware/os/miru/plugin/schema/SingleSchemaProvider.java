package com.jivesoftware.os.miru.plugin.schema;

import com.google.common.base.Preconditions;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.cluster.schema.MiruSchemaProvider;

/**
 *
 */
public class SingleSchemaProvider implements MiruSchemaProvider {

    private final MiruSchema schema;

    public SingleSchemaProvider(MiruSchema schema) {
        this.schema = Preconditions.checkNotNull(schema);
    }

    @Override
    public MiruSchema getSchema(MiruTenantId miruTenantId) {
        return schema;
    }
}
