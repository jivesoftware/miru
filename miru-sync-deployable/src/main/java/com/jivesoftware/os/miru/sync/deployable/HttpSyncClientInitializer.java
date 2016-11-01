package com.jivesoftware.os.miru.sync.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.api.sync.MiruSyncClient;
import com.jivesoftware.os.routing.bird.http.client.HttpClient;
import com.jivesoftware.os.routing.bird.http.client.HttpClientConfig;
import com.jivesoftware.os.routing.bird.http.client.HttpClientConfiguration;
import com.jivesoftware.os.routing.bird.http.client.HttpClientFactory;
import com.jivesoftware.os.routing.bird.http.client.HttpClientFactoryProvider;
import com.jivesoftware.os.routing.bird.http.client.HttpClientSSLConfig;
import com.jivesoftware.os.routing.bird.http.client.HttpRequestHelper;
import java.util.List;

/**
 *
 */
public class HttpSyncClientInitializer {

    public MiruSyncClient initialize(MiruSyncConfig config, ObjectMapper mapper) {
        String schemeHostPort = config.getSyncSenderSchemeHostPort();

        String[] parts = schemeHostPort.split(":");
        String scheme = parts[0];
        String host = parts[1];
        int port = Integer.parseInt(parts[2]);

        List<HttpClientConfiguration> configs = Lists.newArrayList();
        HttpClientConfig httpClientConfig = HttpClientConfig.newBuilder()
            .setSocketTimeoutInMillis(config.getSyncSenderSocketTimeout())
            .build();
        configs.add(httpClientConfig);

        if (scheme.equals("https")) {
            HttpClientSSLConfig sslConfig = HttpClientSSLConfig.newBuilder()
                .setUseSSL(true)
                .build();
            configs.add(sslConfig);
        }
        HttpClientFactory clientFactory = new HttpClientFactoryProvider().createHttpClientFactory(configs, false);

        HttpClient httpClient = clientFactory.createClient(null, host, port);

        return new HttpSyncClient(new HttpRequestHelper(httpClient, mapper), "/api/sync/v1/write/activities", "/api/sync/v1/write/reads");
    }
}
