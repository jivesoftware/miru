package com.jivesoftware.os.miru.service.index;

import com.jivesoftware.os.filer.map.store.api.Copyable;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import java.util.concurrent.Semaphore;

/**
 *
 */
public class HybridIndex<V extends Copyable<V, ?>> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private static final int PERMITS = 64;

    private final String name;
    private final Semaphore semaphore = new Semaphore(PERMITS);

    private V primaryStore;
    private V secondaryStore;

    public HybridIndex(String name, V primaryStore, V secondaryStore) {
        this.name = name;
        this.primaryStore = primaryStore;
        this.secondaryStore = secondaryStore;
    }

    public V acquire() throws InterruptedException {
        if (primaryStore != null) {
            semaphore.acquire();
            return primaryStore;
        } else {
            return secondaryStore;
        }
    }

    public void release(V store) {
        if (store != secondaryStore) {
            semaphore.release();
        }
    }

    public void migrate() throws Exception {
        if (primaryStore == null || secondaryStore == null) {
            return;
        }
        semaphore.acquire(PERMITS);
        try {
            primaryStore.copyTo(secondaryStore);
            LOG.debug("Migrated {}", name);
            primaryStore = null;
        } finally {
            semaphore.release(PERMITS);
        }
    }
}
