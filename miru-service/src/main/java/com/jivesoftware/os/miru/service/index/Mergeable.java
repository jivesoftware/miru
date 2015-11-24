package com.jivesoftware.os.miru.service.index;

import com.jivesoftware.os.filer.io.api.StackBuffer;

/**
 *
 */
public interface Mergeable {

    void merge(StackBuffer stackBuffer) throws Exception;
}
