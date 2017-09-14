package com.jivesoftware.os.miru.sync.deployable;

import com.jivesoftware.os.miru.sync.api.MiruSyncTenantConfig;
import com.jivesoftware.os.miru.sync.api.MiruSyncTenantTuple;
import java.util.Map;

/**
 * Created by jonathan.colt on 12/22/16.
 */
public interface MiruSyncConfigProvider {
     Map<MiruSyncTenantTuple, MiruSyncTenantConfig> getAll(String senderName) throws Exception;
}
