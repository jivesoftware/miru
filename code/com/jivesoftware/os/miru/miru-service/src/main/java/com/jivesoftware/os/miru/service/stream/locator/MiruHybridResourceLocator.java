package com.jivesoftware.os.miru.service.stream.locator;

import java.io.IOException;

/**
 *
 */
public interface MiruHybridResourceLocator extends MiruResourceLocator {

    MiruResourcePartitionIdentifier acquire() throws IOException;

    void release(MiruResourcePartitionIdentifier identifier);
}
