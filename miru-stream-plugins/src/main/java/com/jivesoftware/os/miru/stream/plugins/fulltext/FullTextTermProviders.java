package com.jivesoftware.os.miru.stream.plugins.fulltext;

import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchemaProvider;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import java.util.Collection;
import java.util.Map;

/**
 *
 */
public class FullTextTermProviders {

    private final Map<String, FullTextTermProvider> schemaToProvider = Maps.newConcurrentMap();

    public void addProvider(FullTextTermProvider provider) {
        Collection<String> schemaNames = provider.getSupportedSchemaNames();
        for (String schemaName : schemaNames) {
            schemaToProvider.put(schemaName, provider);
        }
    }

    public FullTextTermProvider get(String schemaName) throws Exception {
        return schemaToProvider.get(schemaName);
    }
}
