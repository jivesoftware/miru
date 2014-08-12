package com.jivesoftware.os.miru.api;

import com.google.common.collect.Multimap;
import com.jivesoftware.os.miru.api.base.MiruTenantId;

/** This interface provides the method to read config from miru. */
public interface MiruConfigReader {

    public static final String CONFIG_SERVICE_ENDPOINT_PREFIX = "/miru/config";

    public static final String PARTITIONS_ENDPOINT = "/partitions";

    Multimap<MiruPartitionState, MiruPartition> getPartitionsForTenant(MiruTenantId tenantId);
}
