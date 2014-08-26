package com.jivesoftware.os.miru.service.stream.locator;

import com.google.common.collect.Sets;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import java.io.File;
import java.util.Set;

/**
 *
 */
public class MiruHybridResourceCleaner {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final File basePath;
    private final Set<String> acquired = Sets.newHashSet();

    public MiruHybridResourceCleaner(final File basePath) {
        this.basePath = basePath;
    }

    public void clean() {
        try {
            Set<File> removalSet = Sets.newHashSet();
            synchronized (acquired) {
                for (File file : basePath.listFiles()) {
                    if (!acquired.contains(file.getName())) {
                        removalSet.add(file);
                    }
                }
            }
            for (File remove : removalSet) {
                log.info("Cleaning directory " + remove.getAbsolutePath());
                FileUtil.remove(remove);
            }
        } catch (Throwable t) {
            log.error("Cleanup thread encountered a problem", t);
        }
    }

    public void acquired(String name) {
        synchronized (acquired) {
            acquired.add(name);
        }
    }

    public void released(String name) {
        synchronized (acquired) {
            acquired.remove(name);
        }
    }
}
