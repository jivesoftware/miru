/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.wiki.miru.deployable;

import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.topology.MiruClusterClient;
import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author jonathan.colt
 */
public class WikiSchemaService {

    private final MiruClusterClient client;
    private final Set<MiruTenantId> registered = new HashSet<>();

    public WikiSchemaService(MiruClusterClient client) {
        this.client = client;
    }

    public void ensureSchema(MiruTenantId tenantId, MiruSchema schema) throws Exception {
        if (!registered.contains(tenantId)) {
            client.registerSchema(tenantId, schema);
            registered.add(tenantId);
        }
    }
}
