package com.jivesoftware.os.miru.plugin.context;

import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;

/**
 *
 */
public interface RequestContextCallback {

    void call(MiruRequestContext<?, ?> requestContext) throws Exception;
}
