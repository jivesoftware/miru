/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.miru.anomaly.deployable;

import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.topology.MiruClusterClient;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author jonathan.colt
 */
public class AnomalySchemaService {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruClusterClient client;
    private final Set<MiruTenantId> registered = new HashSet<>();

    public AnomalySchemaService(MiruClusterClient client) {
        this.client = client;
    }

    public void ensureSchema(MiruTenantId tenantId, MiruSchema schema) throws Exception {
        if (!registered.contains(tenantId)) {
            LOG.info("submitting schema for service:" + tenantId);
            client.registerSchema(tenantId, schema);
            registered.add(tenantId);
        }
    }
}
