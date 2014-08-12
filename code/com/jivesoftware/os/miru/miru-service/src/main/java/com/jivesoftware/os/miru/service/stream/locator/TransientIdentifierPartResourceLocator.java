package com.jivesoftware.os.miru.service.stream.locator;

import com.google.common.io.BaseEncoding;
import com.google.inject.name.Named;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import javax.inject.Inject;
import javax.inject.Singleton;

/**
 *
 */
@Singleton
public class TransientIdentifierPartResourceLocator extends AbstractIdentifierPartResourceLocator implements MiruTransientResourceLocator {

    private final MiruTransientResourceCleaner cleaner;
    private final Random random = new Random();

    @Inject
    public TransientIdentifierPartResourceLocator(
        @Named("miruTransientResourceLocatorPath") String path,
        @Named("miruTransientResourceInitialChunkSize") long initialChunkSize,
        @Named("miruScheduledExecutor") ScheduledExecutorService executor) {
        super(path, initialChunkSize);

        this.cleaner = new MiruTransientResourceCleaner(basePath, executor);
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
