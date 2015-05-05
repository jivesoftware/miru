/*
 * Copyright 2015 jonathan.colt.
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
package com.jivesoftware.os.miru.stumptown.deployable;

import com.jivesoftware.os.miru.api.MiruWriterEndpointConstants;
import com.jivesoftware.os.rcvs.api.RowColumnValueStoreInitializer;
import com.jivesoftware.os.rcvs.api.RowColumnValueStoreProvider;
import org.merlin.config.Config;
import org.merlin.config.defaults.ClassDefault;
import org.merlin.config.defaults.StringDefault;

/**
 *
 * @author jonathan.colt
 */
public interface MiruStumptownServiceConfig extends Config {

    @StringDefault(MiruWriterEndpointConstants.INGRESS_PREFIX + MiruWriterEndpointConstants.ADD)
    public String getMiruIngressEndpoint();

    @StringDefault("unspecifiedHost:0")
    public String getMiruWriterHosts();

    @StringDefault("unspecifiedHost:0")
    public String getMiruReaderHosts();


    @ClassDefault(IllegalStateException.class)
    <C extends Config, I extends RowColumnValueStoreInitializer<E>, E extends Exception> Class<RowColumnValueStoreProvider<C, I, E>>
    getRowColumnValueStoreProviderClass();

}
