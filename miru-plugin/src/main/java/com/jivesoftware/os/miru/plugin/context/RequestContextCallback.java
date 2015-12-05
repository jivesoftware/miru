package com.jivesoftware.os.miru.plugin.context;

import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruPartitionState;

/**
 *
 */
public interface RequestContextCallback {

    void call(MiruRequestContext<?, ?, ?> requestContext) throws Exception;
}
