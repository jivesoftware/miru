package com.jivesoftware.os.miru.cluster.rcvs;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.cluster.schema.MiruSchemaProvider;
import com.jivesoftware.os.miru.cluster.schema.MiruSchemaUnvailableException;
import com.jivesoftware.os.rcvs.api.RowColumnValueStore;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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
    public MiruSchema getSchema(final MiruTenantId tenantId) throws MiruSchemaUnvailableException {
        try {
            return schemaCache.get(tenantId, new Callable<MiruSchema>() {
                @Override
                public MiruSchema call() throws Exception {
                    final AtomicReference<MiruSchema> schema = new AtomicReference<>();
                    schemaRegistry.getValues(MiruVoidByte.INSTANCE, tenantId, new MiruSchemaColumnKey(null, Long.MAX_VALUE), 1L, 1, false, null, null,
                        new CallbackStream<MiruSchema>() {
                            @Override
                            public MiruSchema callback(MiruSchema miruSchema) throws Exception {
                                if (miruSchema != null) {
                                    schema.set(miruSchema);
                                }
                                return null; // always done after one
                            }
                        });
                    if (schema.get() != null) {
                        return schema.get();
                    } else {
                        throw new RuntimeException("Tenant not registered");
                    }
                }
            });
        } catch (UncheckedExecutionException | ExecutionException e) {
            throw new MiruSchemaUnvailableException(e);
        }
    }

    public void register(MiruTenantId tenantId, MiruSchema schema) throws Exception {
        schemaRegistry.add(MiruVoidByte.INSTANCE, tenantId, new MiruSchemaColumnKey(schema.getName(), schema.getVersion()), schema, null, null);
        schemaCache.invalidate(tenantId);
    }
}
