package com.jivesoftware.os.miru.api;

import com.jivesoftware.os.miru.api.base.MiruTenantId;
import java.util.List;

/** This interface provides the method to read config from miru. */
public interface MiruConfigReader {

    public static final String CONFIG_SERVICE_ENDPOINT_PREFIX = "/miru/config";

    public static final String PARTITIONS_ENDPOINT = "/partitions";

    public static final String PRIORITIZE_REBUILD_ENDPOINT = "/rebuild/prioritize";

    List<MiruPartition> getPartitionsForTenant(MiruTenantId tenantId);
}
