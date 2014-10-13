package com.jivesoftware.os.miru.service.schema;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.cluster.rcvs.MiruSchemaColumnKey;
import com.jivesoftware.os.miru.cluster.rcvs.MiruVoidByte;
import com.jivesoftware.os.miru.plugin.schema.MiruSchemaProvider;
import com.jivesoftware.os.miru.plugin.schema.MiruSchemaUnvailableException;
import com.jivesoftware.os.rcvs.api.RowColumnValueStore;
import com.jivesoftware.os.rcvs.api.timestamper.ConstantTimestamper;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class RegistrySchemaProvider implements MiruSchemaProvider {

    private final RowColumnValueStore<MiruVoidByte, MiruTenantId, MiruSchemaColumnKey, MiruSchema, ? extends Exception> schemaRegistry;
    private final Cache<MiruTenantId, MiruSchema> schemaCache;

    public RegistrySchemaProvider(RowColumnValueStore<MiruVoidByte, MiruTenantId, MiruSchemaColumnKey, MiruSchema, ? extends Exception> schemaRegistry,
        int maxInMemory) {

        this.schemaRegistry = schemaRegistry;
        this.schemaCache = CacheBuilder.newBuilder()
            .concurrencyLevel(24)
            .expireAfterAccess(1, TimeUnit.HOURS)
            .maximumSize(maxInMemory)
            .build();
    }

    @Override
    public MiruSchema getSchema(final MiruTenantId miruTenantId) throws MiruSchemaUnvailableException {
        try {
            return schemaCache.get(miruTenantId, new Callable<MiruSchema>() {
                @Override
                public MiruSchema call() throws Exception {
                    MiruSchema schema = schemaRegistry.get(MiruVoidByte.INSTANCE, miruTenantId, MiruSchemaColumnKey.schema, null, null);
                    if (schema != null) {
                        return schema;
                    } else {
                        throw new RuntimeException("Tenant not registered");
                    }
                }
            });
        } catch (UncheckedExecutionException | ExecutionException e) {
            throw new MiruSchemaUnvailableException(e);
        }
    }

    public void register(MiruTenantId tenantId, MiruSchema schema, long version) throws Exception {
        schemaRegistry.add(MiruVoidByte.INSTANCE, tenantId, MiruSchemaColumnKey.schema, schema, null, new ConstantTimestamper(version));
    }
}
