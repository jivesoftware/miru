package com.jivesoftware.os.miru.service.stream.locator;

import com.google.inject.name.Named;
import javax.inject.Inject;
import javax.inject.Singleton;

/**
 *
 */
@Singleton
public class DiskIdentifierPartResourceLocator extends AbstractIdentifierPartResourceLocator {

    @Inject
    public DiskIdentifierPartResourceLocator(
        @Named("miruDiskResourceLocatorPath") String path,
        @Named("miruDiskResourceInitialChunkSize") long initialChunkSize) {
        super(path, initialChunkSize);
    }

}
