package com.jivesoftware.os.miru.siphon.deployable;

import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.topology.MiruClusterClient;
import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author jonathan.colt
 */
public class MiruSiphonSchemaService {

    private final MiruClusterClient client;
    private final Set<MiruTenantId> registered = new HashSet<>();

    public MiruSiphonSchemaService(MiruClusterClient client) {
        this.client = client;
    }

    public boolean ensured(MiruTenantId tenantId) {
        return registered.contains(tenantId);
    }

    public void ensureSchema(MiruTenantId tenantId, MiruSchema schema) throws Exception {
        if (!ensured(tenantId)) {
            client.registerSchema(tenantId, schema);
            registered.add(tenantId);
        }
    }
}
