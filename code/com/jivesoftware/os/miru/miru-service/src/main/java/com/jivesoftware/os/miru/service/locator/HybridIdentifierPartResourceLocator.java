package com.jivesoftware.os.miru.service.locator;

import com.google.common.io.BaseEncoding;
import java.io.File;
import java.util.Random;

/**
 *
 */
public class HybridIdentifierPartResourceLocator extends AbstractIdentifierPartResourceLocator implements MiruHybridResourceLocator {

    private final MiruHybridResourceCleaner cleaner;
    private final Random random = new Random();

    public HybridIdentifierPartResourceLocator(File[] paths,
        long initialChunkSize,
        MiruHybridResourceCleaner cleaner) {
        super(paths, initialChunkSize);
        this.cleaner = cleaner;
    }

    @Override
    public MiruResourcePartitionIdentifier acquire() {
        MiruHybridTokenIdentifier identifier = null;
        while (identifier == null || anyExist(getPartitionPaths(identifier))) {
            byte[] bytes = new byte[12];
            random.nextBytes(bytes);
            identifier = new MiruHybridTokenIdentifier(BaseEncoding.base64Url().omitPadding().encode(bytes));
        }
        cleaner.acquired(identifier.getToken());
        return identifier;
    }

    private boolean anyExist(File[] partitionPaths) {
        for (File partitionPath : partitionPaths) {
            if (partitionPath.exists()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void release(MiruResourcePartitionIdentifier identifier) {
        if (!(identifier instanceof MiruHybridTokenIdentifier)) {
            throw new IllegalArgumentException("Unknown identifier type");
        }

        cleaner.released(((MiruHybridTokenIdentifier) identifier).getToken());
    }
}
