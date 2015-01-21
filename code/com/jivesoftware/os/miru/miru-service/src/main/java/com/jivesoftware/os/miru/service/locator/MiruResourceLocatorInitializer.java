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
import com.jivesoftware.os.miru.service.MiruServiceConfig;
import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.merlin.config.defaults.LongDefault;
import org.merlin.config.defaults.StringDefault;

/**
 * @author jonathan
 */
public class MiruResourceLocatorInitializer {

    static interface ResidentResourceDiskCheck extends ScheduledMinMaxHealthCheckConfig {

        @StringDefault("resident>resource>disk")
        @Override
        public String getName();

        @LongDefault(80)
        @Override
        public Long getMax();

    }

    public MiruResourceLocator initialize(final MiruServiceConfig config) throws IOException {

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
            config.getOnDiskInitialChunkSize(),
            config.getInMemoryChunkSize());

        final File[] transientPaths = pathToFile(config.getTransientResourceLocatorPaths().split(","));
        for (File transientPath : transientPaths) {
            FileUtils.forceMkdir(transientPath);
        }

        return residentDiskResourceLocator;
    }

    private File[] pathToFile(String[] paths) {
        File[] files = new File[paths.length];
        for (int i = 0; i < files.length; i++) {
            files[i] = new File(paths[i].trim());
        }
        return files;
    }
}
