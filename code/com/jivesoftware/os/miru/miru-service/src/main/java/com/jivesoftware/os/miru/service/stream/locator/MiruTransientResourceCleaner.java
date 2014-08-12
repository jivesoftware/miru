package com.jivesoftware.os.miru.service.stream.locator;

import com.google.common.collect.Sets;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import java.io.File;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class MiruTransientResourceCleaner {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final Set<String> acquired = Sets.newHashSet();

    public MiruTransientResourceCleaner(final File basePath, ScheduledExecutorService executor) {
        executor.scheduleWithFixedDelay(
            new Runnable() {
                @Override
                public void run() {
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
            },
            //TODO config?
            0, 5, TimeUnit.MINUTES);
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
