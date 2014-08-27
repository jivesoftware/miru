package com.jivesoftware.os.miru.service.stream.locator;

import com.google.common.io.BaseEncoding;
import java.io.File;
import java.util.Random;

/**
 *
 */
public class HybridIdentifierPartResourceLocator extends AbstractIdentifierPartResourceLocator implements MiruHybridResourceLocator {

    private final MiruHybridResourceCleaner cleaner;
    private final Random random = new Random();

    public HybridIdentifierPartResourceLocator(File path,
        long initialChunkSize,
        MiruHybridResourceCleaner cleaner) {
        super(path, initialChunkSize);
        this.cleaner = cleaner;
    }

    @Override
    public MiruResourcePartitionIdentifier acquire() {
        MiruHybridTokenIdentifier identifier = null;
        while (identifier == null || getPartitionPath(identifier).exists()) {
            byte[] bytes = new byte[12];
            random.nextBytes(bytes);
            identifier = new MiruHybridTokenIdentifier(BaseEncoding.base64Url().omitPadding().encode(bytes));
        }
        cleaner.acquired(identifier.getToken());
        return identifier;
    }

    @Override
    public void release(MiruResourcePartitionIdentifier identifier) {
        if (!(identifier instanceof MiruHybridTokenIdentifier)) {
            throw new IllegalArgumentException("Unknown identifier type");
        }

        cleaner.released(((MiruHybridTokenIdentifier) identifier).getToken());
    }
}
