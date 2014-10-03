package com.jivesoftware.os.miru.service.locator;

import java.io.File;

/**
 *
 */
public class DiskIdentifierPartResourceLocator extends AbstractIdentifierPartResourceLocator {

    public DiskIdentifierPartResourceLocator(File[] paths, long initialChunkSize) {
        super(paths, initialChunkSize);
    }

}
