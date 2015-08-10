package com.jivesoftware.os.miru.api.activity;

import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.base.MiruTenantId;

/**
 *
 */
public interface CoordinateStream {

    boolean stream(MiruTenantId tenantId, MiruPartitionId partitionId, MiruHost host) throws Exception;
}
