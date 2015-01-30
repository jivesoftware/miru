package com.jivesoftware.os.miru.logappender;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.jivesoftware.os.jive.utils.http.client.HttpClient;
import com.jivesoftware.os.jive.utils.http.client.HttpClientConfig;
import com.jivesoftware.os.jive.utils.http.client.HttpClientConfiguration;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactory;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactoryProvider;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import java.util.Collection;
import org.merlin.config.Config;
import org.merlin.config.defaults.BooleanDefault;
import org.merlin.config.defaults.IntDefault;
import org.merlin.config.defaults.LongDefault;
import org.merlin.config.defaults.StringDefault;

/**
 *
 */
public class MiruLogAppenderInitializer {

    public static interface MiruLogAppenderConfig extends Config {

        @StringDefault("undefined")
        String getMiruLumberyardHostPorts();

        @IntDefault(60_000)
        int getSocketTimeoutInMillis();

        @IntDefault(-1)
        int getMaxConnections();

        @IntDefault(1)
        int getMaxConnectionsPerHost();

        @IntDefault(100_000)
        int getQueueMaxDepth();

        @BooleanDefault(false)
        boolean getQueueIsBlocking();

        @LongDefault(1_000)
        long getIfSuccessPauseMillis();

        @LongDefault(1_000)
        long getIfEmptyPauseMillis();

        @LongDefault(5_000)
        long getIfErrorPauseMillis();

        @LongDefault(1_000)
        long getCycleReceiverAfterAppendCount();

        @IntDefault(1_000)
        int getNonBlockingDrainThreshold();

        @IntDefault(10_000)
        int getNonBlockingDrainCount();
    }

    public MiruLogAppender initialize(String datacenter,
        String cluster,
        String host,
        String service,
        String instance,
        String version,
        MiruLogAppenderConfig config,
        ObjectMapper objectMapper) {

        String[] hostPorts = config.getMiruLumberyardHostPorts().split("\\s*,\\s*");
        RequestHelper[] requestHelpers = new RequestHelper[hostPorts.length];

        // MiruConfigReader client configuration
        Collection<HttpClientConfiguration> httpClientConfigurations = Lists.newArrayList();
        HttpClientConfig httpClientConfig = HttpClientConfig.newBuilder()
            .setSocketTimeoutInMillis(config.getSocketTimeoutInMillis())
            .setMaxConnections(config.getMaxConnections())
            .setMaxConnectionsPerHost(config.getMaxConnectionsPerHost())
            .build();
        httpClientConfigurations.add(httpClientConfig);

        HttpClientFactory httpClientFactory = new HttpClientFactoryProvider().createHttpClientFactory(httpClientConfigurations);

        for (int i = 0; i < requestHelpers.length; i++) {
            String[] parts = hostPorts[i].split(":");
            HttpClient httpClient = httpClientFactory.createClient(parts[0], Integer.parseInt(parts[1]));
            requestHelpers[i] = new RequestHelper(httpClient, objectMapper);
        }

        return new MiruLogAppender(datacenter,
            cluster,
            host,
            service,
            instance,
            version,
            requestHelpers,
            config.getQueueMaxDepth(),
            config.getQueueIsBlocking(),
            config.getIfSuccessPauseMillis(),
            config.getIfEmptyPauseMillis(),
            config.getIfErrorPauseMillis(),
            config.getCycleReceiverAfterAppendCount(),
            config.getNonBlockingDrainThreshold(),
            config.getNonBlockingDrainCount());
    }

}
