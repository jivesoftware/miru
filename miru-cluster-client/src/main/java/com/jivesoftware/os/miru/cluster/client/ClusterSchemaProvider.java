package com.jivesoftware.os.miru.cluster.client;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchemaProvider;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchemaUnavailableException;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.topology.MiruClusterClient;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class ClusterSchemaProvider implements MiruSchemaProvider {

    private final MiruClusterClient client;
    private final Cache<MiruTenantId, MiruSchema> cache;

    public ClusterSchemaProvider(MiruClusterClient client,
        int maxInMemory) {

        this.client = client;
        this.cache = CacheBuilder.newBuilder()
            .concurrencyLevel(24)
            .expireAfterWrite(5, TimeUnit.MINUTES)
            .maximumSize(maxInMemory)
            .build();
    }

    @Override
    public MiruSchema getSchema(final MiruTenantId tenantId) throws MiruSchemaUnavailableException {
        try {
            return cache.get(tenantId, () -> {
                MiruSchema schema = client.getSchema(tenantId);
                if (schema != null) {
                    return schema;
                } else {
                    throw new RuntimeException("Tenant not registered");
                }
            });
        } catch (UncheckedExecutionException | ExecutionException e) {
            throw new MiruSchemaUnavailableException(e);
        }
    }

    public void register(MiruTenantId tenantId, MiruSchema schema) throws Exception {
        client.registerSchema(tenantId, schema);
        cache.invalidate(tenantId);
    }
}
