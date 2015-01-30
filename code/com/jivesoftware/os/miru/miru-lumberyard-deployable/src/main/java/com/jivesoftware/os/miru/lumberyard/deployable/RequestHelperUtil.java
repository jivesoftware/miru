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
package com.jivesoftware.os.miru.lumberyard.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.jive.utils.http.client.HttpClient;
import com.jivesoftware.os.jive.utils.http.client.HttpClientConfig;
import com.jivesoftware.os.jive.utils.http.client.HttpClientConfiguration;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactory;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactoryProvider;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import java.util.Arrays;

/**
 *
 * @author jonathan.colt
 */
public class RequestHelperUtil {

    public static RequestHelper[] buildRequestHelpers(String hostPortTuples, ObjectMapper mapper) {
        String[] hostPorts = hostPortTuples.split(",");
        RequestHelper[] helpers = new RequestHelper[hostPorts.length];
        for (int i = 0; i < helpers.length; i++) {
            String[] hostAndPort = hostPorts[i].split(":");
            helpers[i] = buildRequestHelper(hostAndPort[0], Integer.parseInt(hostAndPort[1]), mapper);
        }
        return helpers;
    }

    private static RequestHelper buildRequestHelper(String host, int port, ObjectMapper mapper) {
        HttpClientConfig httpClientConfig = HttpClientConfig.newBuilder().build();
        HttpClientFactory httpClientFactory = new HttpClientFactoryProvider().createHttpClientFactory(Arrays.<HttpClientConfiguration>asList(httpClientConfig));
        HttpClient httpClient = httpClientFactory.createClient(host, port);
        return new RequestHelper(httpClient, mapper);
    }
}
