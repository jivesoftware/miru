package com.jivesoftware.os.miru.service.index;

import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;

/**
 *
 */
public interface Mergeable {

    void merge(MiruSchema schema, StackBuffer stackBuffer) throws Exception;
}
