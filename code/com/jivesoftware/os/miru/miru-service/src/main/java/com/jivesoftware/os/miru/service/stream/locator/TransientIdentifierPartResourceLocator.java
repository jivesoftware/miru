package com.jivesoftware.os.miru.service.stream.locator;

import com.google.common.io.BaseEncoding;
import java.io.File;
import java.util.Random;
import javax.inject.Singleton;

/**
 *
 */
@Singleton
public class TransientIdentifierPartResourceLocator extends AbstractIdentifierPartResourceLocator implements MiruTransientResourceLocator {

    private final MiruTransientResourceCleaner cleaner;
    private final Random random = new Random();

    public TransientIdentifierPartResourceLocator(File path,
        long initialChunkSize,
        MiruTransientResourceCleaner cleaner) {
        super(path, initialChunkSize);
        this.cleaner = cleaner;
    }

    @Override
    public MiruResourcePartitionIdentifier acquire() {
        MiruTransientTokenIdentifier identifier = null;
        while (identifier == null || getPartitionPath(identifier).exists()) {
            byte[] bytes = new byte[12];
            random.nextBytes(bytes);
            identifier = new MiruTransientTokenIdentifier(BaseEncoding.base64Url().omitPadding().encode(bytes));
        }
        cleaner.acquired(identifier.getToken());
        return identifier;
    }

    @Override
    public void release(MiruResourcePartitionIdentifier identifier) {
        if (!(identifier instanceof MiruTransientTokenIdentifier)) {
            throw new IllegalArgumentException("Unknown identifier type");
        }

        cleaner.released(((MiruTransientTokenIdentifier) identifier).getToken());
    }
}
