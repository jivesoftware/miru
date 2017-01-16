package com.jivesoftware.os.miru.sync.deployable;

import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.sync.api.MiruSyncTenantConfig;
import java.util.Map;

/**
 * Created by jonathan.colt on 12/22/16.
 */
public interface MiruSyncConfigProvider {

     Map<MiruTenantId, MiruSyncTenantConfig> getAll(String senderName) throws Exception;
}
