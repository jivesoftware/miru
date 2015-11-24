package com.jivesoftware.os.miru.plugin.context;

/**
 *
 */
public interface RequestContextCallback {

    void call(MiruRequestContext<?, ?> requestContext) throws Exception;
}
