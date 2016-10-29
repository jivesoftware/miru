package com.jivesoftware.os.miru.bot.deployable;

import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.topology.MiruClusterClient;

import java.util.HashSet;
import java.util.Set;

class MiruBotSchemaService {

    private final MiruClusterClient client;
    private final Set<MiruTenantId> registered = new HashSet<>();

    MiruBotSchemaService(MiruClusterClient client) {
        this.client = client;
    }

    void ensureSchema(MiruTenantId miruTenantId, MiruSchema miruSchema) throws Exception {
        if (!registered.contains(miruTenantId)) {
            client.registerSchema(miruTenantId, miruSchema);
            registered.add(miruTenantId);
        }
    }
}
