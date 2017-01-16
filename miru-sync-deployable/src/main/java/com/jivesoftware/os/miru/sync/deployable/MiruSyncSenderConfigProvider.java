package com.jivesoftware.os.miru.sync.deployable;

import com.jivesoftware.os.miru.sync.api.MiruSyncSenderConfig;
import java.util.Map;

/**
 * Created by jonathan.colt on 12/22/16.
 */
public interface MiruSyncSenderConfigProvider {

     Map<String, MiruSyncSenderConfig> getAll() throws Exception;
}
