/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.miru.stumptown.deployable;

import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.miru.api.MiruConfigReader;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 *
 * @author jonathan.colt
 */
public class StumptownSchemaService {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final Random rand = new Random();
    private final RequestHelper[] miruReaderHosts;
    private final Set<MiruTenantId> registered = new HashSet<>();

    public StumptownSchemaService(RequestHelper[] miruReaderHosts) {
        this.miruReaderHosts = miruReaderHosts;
    }

    public void ensureSchema(MiruTenantId serviceId, MiruSchema schema) {
        if (!registered.contains(serviceId)) {
            int index = rand.nextInt(miruReaderHosts.length);
            LOG.info("submitting schema for service:" + serviceId + " to " + miruReaderHosts[index]);
            RequestHelper requestHelper = miruReaderHosts[index];
            requestHelper.executeRequest(schema, MiruConfigReader.CONFIG_SERVICE_ENDPOINT_PREFIX + "/schema/" + serviceId.toString(), String.class, null);
            registered.add(serviceId);
        }
    }
}
