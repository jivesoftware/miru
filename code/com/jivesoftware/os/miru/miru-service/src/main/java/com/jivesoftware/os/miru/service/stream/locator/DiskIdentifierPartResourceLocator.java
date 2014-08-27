package com.jivesoftware.os.miru.service.stream.locator;

import java.io.File;

/**
 *
 */
public class DiskIdentifierPartResourceLocator extends AbstractIdentifierPartResourceLocator {

    public DiskIdentifierPartResourceLocator(File path, long initialChunkSize) {
        super(path, initialChunkSize);
    }

}
