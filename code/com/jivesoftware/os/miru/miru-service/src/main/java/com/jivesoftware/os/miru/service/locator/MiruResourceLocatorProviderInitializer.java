/*
 * Copyright 2014 jonathan.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jivesoftware.os.miru.service.locator;

import com.jivesoftware.os.jive.utils.health.api.HealthChecker;
import com.jivesoftware.os.jive.utils.health.api.HealthFactory;
import com.jivesoftware.os.jive.utils.health.api.ScheduledMinMaxHealthCheckConfig;
import com.jivesoftware.os.jive.utils.health.checkers.DiskFreeHealthChecker;
import com.jivesoftware.os.jive.utils.logger.Counter;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.MiruLifecyle;
import com.jivesoftware.os.miru.service.MiruServiceConfig;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.merlin.config.defaults.LongDefault;
import org.merlin.config.defaults.StringDefault;

/**
 * @author jonathan
 */
public class MiruResourceLocatorProviderInitializer {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    static interface ResidentResourceDiskCheck extends ScheduledMinMaxHealthCheckConfig {

        @StringDefault ("resident>resource>disk")
        @Override
        public String getName();

        @LongDefault (80)
        @Override
        public Long getMax();

    }

    static interface TransientResourceDiskCheck extends ScheduledMinMaxHealthCheckConfig {

        @StringDefault ("transient>resource>disk")
        @Override
        public String getName();

        @LongDefault (80)
        @Override
        public Long getMax();

    }

    public MiruLifecyle<MiruResourceLocatorProvider> initialize(final MiruServiceConfig config) throws IOException {

        final ScheduledExecutorService scheduledExecutor = Executors.newScheduledThreadPool(6); //TODO expose to config

        final File[] residentDiskPaths = pathToFile(config.getDiskResourceLocatorPaths().split(","));
        for (File residentDiskPath : residentDiskPaths) {
            FileUtils.forceMkdir(residentDiskPath);
        }

        HealthFactory.scheduleHealthChecker(ResidentResourceDiskCheck.class,
            new HealthFactory.HealthCheckerConstructor<Counter, ResidentResourceDiskCheck>() {

                @Override
                public HealthChecker<Counter> construct(ResidentResourceDiskCheck config) {
                    return new DiskFreeHealthChecker(config, residentDiskPaths);
                }
            });

        final MiruResourceLocator residentDiskResourceLocator = new DiskIdentifierPartResourceLocator(
            residentDiskPaths,
            config.getDiskResourceInitialChunkSize());

        final File[] transientPaths = pathToFile(config.getTransientResourceLocatorPaths().split(","));
        for (File transientPath : transientPaths) {
            FileUtils.forceMkdir(transientPath);
        }

        HealthFactory.scheduleHealthChecker(TransientResourceDiskCheck.class,
            new HealthFactory.HealthCheckerConstructor<Counter, TransientResourceDiskCheck>() {

                @Override
                public HealthChecker<Counter> construct(TransientResourceDiskCheck config) {
                    return new DiskFreeHealthChecker(config, transientPaths);
                }
            });

        final MiruHybridResourceCleaner cleaner = new MiruHybridResourceCleaner(transientPaths);
        final MiruHybridResourceLocator transientResourceLocator = new HybridIdentifierPartResourceLocator(
            transientPaths,
            config.getTransientResourceInitialChunkSize(),
            cleaner);

        final MiruResourceLocatorProvider miruResourceLocatorProvider = new MiruResourceLocatorProvider() {

            @Override
            public MiruResourceLocator getDiskResourceLocator() {
                return residentDiskResourceLocator;
            }

            @Override
            public MiruHybridResourceLocator getTransientResourceLocator() {
                return transientResourceLocator;
            }
        };
        return new MiruLifecyle<MiruResourceLocatorProvider>() {

            @Override
            public MiruResourceLocatorProvider getService() {
                return miruResourceLocatorProvider;
            }

            @Override
            public void start() throws Exception {
                scheduledExecutor.scheduleWithFixedDelay(
                    new Runnable() {
                        @Override
                        public void run() {
                            cleaner.clean();
                        }
                    },
                    0, 5, TimeUnit.MINUTES); //TODO config?
            }

            @Override
            public void stop() throws Exception {
                scheduledExecutor.shutdownNow();
            }
        };
    }

    private File[] pathToFile(String[] paths) {
        File[] files = new File[paths.length];
        for (int i = 0; i < files.length; i++) {
            files[i] = new File(paths[i].trim());
        }
        return files;
    }
}
