package com.jivesoftware.os.miru.service.stream.locator;

import java.io.IOException;

/**
 *
 */
public interface MiruTransientResourceLocator extends MiruResourceLocator {

    MiruResourcePartitionIdentifier acquire() throws IOException;

    void release(MiruResourcePartitionIdentifier identifier);
}
