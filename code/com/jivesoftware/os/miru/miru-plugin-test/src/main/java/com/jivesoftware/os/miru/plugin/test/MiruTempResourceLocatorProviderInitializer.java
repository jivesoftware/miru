/*
 * Copyright 2014 jivesoftware.
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
package com.jivesoftware.os.miru.plugin.test;

import com.jivesoftware.os.miru.api.MiruLifecyle;
import com.jivesoftware.os.miru.service.locator.MiruHybridResourceLocator;
import com.jivesoftware.os.miru.service.locator.MiruResourceLocator;
import com.jivesoftware.os.miru.service.locator.MiruResourceLocatorProvider;
import com.jivesoftware.os.miru.service.locator.MiruTempDirectoryResourceLocator;
import java.io.IOException;

/**
 *
 * @author jonathan
 */
public class MiruTempResourceLocatorProviderInitializer {

    public MiruLifecyle<MiruResourceLocatorProvider> initialize() throws IOException {

        final MiruResourceLocator diskResourceLocator = new MiruTempDirectoryResourceLocator();
        final MiruHybridResourceLocator transientResourceLocator = new MiruTempDirectoryResourceLocator();

        return new MiruLifecyle<MiruResourceLocatorProvider>() {

            @Override
            public MiruResourceLocatorProvider getService() {
                return new MiruResourceLocatorProvider() {

                    @Override
                    public MiruResourceLocator getDiskResourceLocator() {
                        return diskResourceLocator;
                    }

                    @Override
                    public MiruHybridResourceLocator getTransientResourceLocator() {
                        return transientResourceLocator;
                    }
                };
            }

            @Override
            public void start() throws Exception {
            }

            @Override
            public void stop() throws Exception {
            }
        };
    }
}
