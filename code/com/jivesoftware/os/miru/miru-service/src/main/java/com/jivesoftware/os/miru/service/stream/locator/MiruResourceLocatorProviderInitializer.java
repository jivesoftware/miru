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
package com.jivesoftware.os.miru.service.stream.locator;

import com.jivesoftware.os.miru.api.MiruLifecyle;
import com.jivesoftware.os.miru.service.MiruServiceConfig;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;

/**
 *
 * @author jonathan
 */
public class MiruResourceLocatorProviderInitializer {

    public MiruLifecyle<MiruResourceLocatorProvider> initialize(final MiruServiceConfig config) throws IOException {

        final ScheduledExecutorService scheduledExecutor = Executors.newScheduledThreadPool(6);//TODO expose to config

        File diskPath = new File(config.getDiskResourceLocatorPath());
        FileUtils.forceMkdir(diskPath);
        final MiruResourceLocator diskResourceLocator = new DiskIdentifierPartResourceLocator(
                diskPath,
                config.getDiskResourceInitialChunkSize());

        File transientPath = new File(config.getTransientResourceLocatorPath());
        FileUtils.forceMkdir(transientPath);
        final MiruHybridResourceCleaner cleaner = new MiruHybridResourceCleaner(transientPath);
        final MiruHybridResourceLocator transientResourceLocator = new HybridIdentifierPartResourceLocator(
                transientPath,
                config.getTransientResourceInitialChunkSize(),
                cleaner);

        final MiruResourceLocatorProvider miruResourceLocatorProvider = new MiruResourceLocatorProvider() {

                    @Override
                    public MiruResourceLocator getDiskResourceLocator() {
                        return diskResourceLocator;
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
                        0, 5, TimeUnit.MINUTES);//TODO config?
            }

            @Override
            public void stop() throws Exception {
                scheduledExecutor.shutdownNow();
            }
        };
    }
}
