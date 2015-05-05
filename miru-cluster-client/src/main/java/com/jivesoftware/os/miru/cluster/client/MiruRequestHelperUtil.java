package com.jivesoftware.os.miru.cluster.client;

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
public class MiruRequestHelperUtil {

    public static RequestHelper[] buildRequestHelpers(String hostPortTuples, ObjectMapper mapper, int socketTimeout, int maxConnections) {
        String[] hostPorts = hostPortTuples.split(",");
        RequestHelper[] helpers = new RequestHelper[hostPorts.length];
        for (int i = 0; i < helpers.length; i++) {
            String[] hostAndPort = hostPorts[i].split(":");
            helpers[i] = buildRequestHelper(hostAndPort[0], Integer.parseInt(hostAndPort[1]), socketTimeout, maxConnections, mapper);
        }
        return helpers;
    }

    private static RequestHelper buildRequestHelper(String host, int port, int socketTimeout, int maxConnections, ObjectMapper mapper) {
        HttpClientConfig httpClientConfig = HttpClientConfig.newBuilder()
            .setSocketTimeoutInMillis(socketTimeout).setMaxConnections(maxConnections)
            .build();
        HttpClientFactory httpClientFactory = new HttpClientFactoryProvider()
            .createHttpClientFactory(Arrays.<HttpClientConfiguration>asList(httpClientConfig));
        HttpClient httpClient = httpClientFactory.createClient(host, port);
        return new RequestHelper(httpClient, mapper);
    }
}
